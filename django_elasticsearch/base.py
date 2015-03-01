# python
from itertools import chain
import logging
import traceback
import pprint
from datetime import datetime

# django
from django.db.backends import connection_created
from django.db import connections, router, transaction, models as dj_models, DEFAULT_DB_ALIAS
from django.utils.datastructures import SortedDict


# djangotoolbox
from djangotoolbox.db.base import (
    NonrelDatabaseClient,
    NonrelDatabaseFeatures,
    NonrelDatabaseIntrospection,
    NonrelDatabaseOperations,
    NonrelDatabaseValidation,
    NonrelDatabaseWrapper,
)

# pyes
from pyes import ES
from pyes.exceptions import IndexAlreadyExistsException, IndexMissingException

# djes
from creation import DatabaseCreation
from schema import DatabaseSchemaEditor
from . import ENGINE, NUMBER_OF_REPLICAS, NUMBER_OF_SHARDS, INTERNAL_INDEX

logger = logging.getLogger(__name__)


__author__ = 'jorgealegre'


class DatabaseFeatures(NonrelDatabaseFeatures):

    string_based_auto_field = True


class DatabaseOperations(NonrelDatabaseOperations):

    compiler_module = 'django_elasticsearch.compiler'

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        for table in tables:
            self.connection.indices.delete_mapping(self.connection.db_name, table)
        return []

    def check_aggregate_support(self, aggregate):
        """
        This function is meant to raise exception if backend does
        not support aggregation.
        """
        pass

    def create_index(self, index_name, options, alias=None, mapping=None):
        """
        Creates index with options as settings

        index_name should contain time created:
        myindex-mm-dd-yyyyTHH:MM:SS with alias myindex

        :param index_name:
        :param options:
        :return:
        :raises IndexAlreadyExistsException when can't create index.
        """
        # "logstash-%{+YYYY.MM.dd}"
        alias = index_name if alias is None else alias
        index_name = u'{}-{}'.format(index_name, datetime.now().strftime("%Y.%m.%dT%H:%M:%S"))
        es_connection = self.connection.connection
        es_connection.indices.create_index(index_name, settings={
            'analysis': options.get('ANALYSIS', {}),
            'number_of_replicas': options.get('NUMBER_OF_REPLICAS', NUMBER_OF_REPLICAS),
            'number_of_shards': options.get('NUMBER_OF_SHARDS', NUMBER_OF_SHARDS),
        })
        # alias
        es_connection.indices.add_alias(alias, index_name)
        # TODO: Implement .django_engine index create
        logger.info(u'index "{}" created'.format(index_name))

    def delete_index(self, index_name):
        """
        Deletes index

        :param index_name:
        :return:
        """
        es_connection = self.connection.connection
        es_connection.indices.delete_index(index_name)

    def rebuild_index(self):
        """
        Rebuilds index in the background

        :return:
        """
        # 1. create alt index
        # 2. create mappings for alt index: mapping.save()
        # 2. export/import data to new index
        # 3. assign alias to new index
        # 4. delete old index
        pass

    def build_django_engine_structure(self):
        from django_elasticsearch.fields import DocumentObjectField, DateField, StringField, IntegerField, \
            ObjectField
        # crete .django_engine index
        try:
            self.create_index(INTERNAL_INDEX, {
                'number_of_replicas': 1,
                'number_of_shards': 5,
            })
        except IndexAlreadyExistsException:
            pass
        # mappings for content index
        mapping_indices = DocumentObjectField(
            name='indices',
            connection=self.connection,
            index_name=INTERNAL_INDEX,
            properties={
                'operation': StringField(index='not_analyzed'),
                'index_name': StringField(index='not_analyzed'),
                'options': ObjectField(),
                'created_on': DateField(),
                'updated_on': DateField(),
            })
        try:
            result = self.connection.indices.put_mapping(doc_type='indices',
                                                         mapping=mapping_indices.as_dict(),
                                                         indices=INTERNAL_INDEX)
            logger.info(u'{} result: {}'.format('.django_engine/indices',
                                                pprint.PrettyPrinter(indent=4).pformat(result)))
        except Exception:
            # MergeMappingException
            traceback.print_exc()
        # mappings for mapping_migration
        mapping_migration = DocumentObjectField(
            name='mapping_migration',
            connection=self.connection,
            index_name=INTERNAL_INDEX,
            properties={
                'operation': StringField(index='not_analyzed'),
                'doc_type': StringField(index='not_analyzed'),
                'mapping_old': StringField(index='not_analyzed'),
                'mapping_new': StringField(index='not_analyzed'),
                'created_on': DateField(),
                'updated_on': DateField(),
            })
        try:
            result = self.connection.indices.put_mapping(doc_type='mapping_migration',
                                                         mapping=mapping_migration.as_dict(),
                                                         indices=INTERNAL_INDEX)
            logger.info(u'{} result: {}'.format('.django_engine/mapping_migration',
                                                pprint.PrettyPrinter(indent=4).pformat(result)))
        except Exception:
            # MergeMappingException
            traceback.print_exc()


class DatabaseClient(NonrelDatabaseClient):
    pass


class DatabaseValidation(NonrelDatabaseValidation):
    pass


class DatabaseIntrospection(NonrelDatabaseIntrospection):

    def __init__(self, *args, **kwargs):
        super(NonrelDatabaseIntrospection, self).__init__(*args, **kwargs)
        self._models = {}
        self._models_discovered = False
        self._mappings = {}

    def _discover_models(self):
        """
        Discover django models and set into _models class attribute
        """
        # db = options.get('database')
        db = DEFAULT_DB_ALIAS
        tables = self.table_names()
        all_models = [
            (app.__name__.split('.')[-2],
                [m for m in dj_models.get_models(app, include_auto_created=True)
                    if router.allow_syncdb(db, m)])
            for app in dj_models.get_apps()
        ]
        logger.debug(u'all_models: {}'.format(all_models))

        def model_installed(model):
            opts = model._meta
            converter = self.table_name_converter
            return not ((converter(opts.db_table) in tables) or
                        (opts.auto_created and converter(opts.auto_created._meta.db_table) in tables))

        manifest = SortedDict(
            (app_name, list(filter(model_installed, model_list)))
            for app_name, model_list in all_models
        )
        for app_name, model_list in manifest.items():
            logger.debug(u'app_name: {} model_list: {}'.format(app_name, model_list))
            app_models = []
            for model in model_list:
                app_models.append(model)
            self._models[app_name] = app_models

    @property
    def models(self):
        if not self._models_discovered:
            self._discover_models()
            self._models_discovered = True
        return self._models

    @property
    def mappings(self):
        return self._mappings

    def django_table_names(self, only_existing=False):
        """
        Returns a list of all table names that have associated cqlengine models
        and are present in settings.INSTALLED_APPS.
        """

        """all_models = list(chain.from_iterable(self.cql_models.values()))
        tables = [model.column_family_name(include_keyspace=False)
                  for model in all_models]"""

        return []

    def table_names(self, cursor=None):
        """
        Returns all table names
        """
        # TODO: get content types from indices
        return []

    def get_table_list(self, cursor):
        return self.table_names()

    def get_table_description(self, *_):
        """
        Get model mapping
        """
        return ""


class DatabaseWrapper(NonrelDatabaseWrapper):

    vendor = 'elasticsearch'

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        # Set up the associated backend objects
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.validation = DatabaseValidation(self)
        self.introspection = DatabaseIntrospection(self)

        self.commit_on_exit = False
        self.connected = False
        self.autocommit = True
        self.es_url = '{}:{}'.format(self.settings_dict['HOST'], self.settings_dict['PORT'])

        del self.connection

    def connect(self):
        print u'connect... es_url: {} options: {}'.format(self.es_url, self.settings_dict)
        if not self.connected or self.connection is None:
            self.connection = ES(self.es_url,
                                 default_indices=[self.settings_dict['NAME']],
                                 bulk_size=1000)
            connection_created.send(sender=self.__class__, connection=self)
            self.connected = True

    def __getattr__(self, attr):
        if attr == "connection":
            assert not self.connected
            self.connect()
            return getattr(self, attr)
        raise AttributeError(attr)

    def reconnect(self):
        if self.connected:
            del self.connection
            self.connected = False
        self.connect()

    def _commit(self):
        pass

    def _rollback(self):
        pass

    def close(self):
        pass

    def schema_editor(self, *args, **kwargs):
        """
        Returns a new instance of this backend's SchemaEditor (Django>=1.7)
        """
        return DatabaseSchemaEditor(self, *args, **kwargs)
