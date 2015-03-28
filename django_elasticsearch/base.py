# python
from itertools import chain
import logging
import traceback
import pprint
from datetime import datetime
import json

# django
from django.db.backends import connection_created
from django.db import connections, router, transaction, models as dj_models, DEFAULT_DB_ALIAS
from django.utils.datastructures import SortedDict
from django.conf import settings
from django.utils.translation import ugettext as _


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
from pyes.exceptions import IndexAlreadyExistsException, IndexMissingException, ElasticSearchException
from pyes.query import Search, QueryStringQuery
import pyes.mappings
from pyes.helpers import SettingsBuilder

# djes
from creation import DatabaseCreation
from schema import DatabaseSchemaEditor
from . import ENGINE, NUMBER_OF_REPLICAS, NUMBER_OF_SHARDS, INTERNAL_INDEX, \
    OPERATION_CREATE_INDEX, OPERATION_DELETE_INDEX, OPERATION_UPDATE_MAPPING
from mapping import model_to_mapping
import exceptions

logger = logging.getLogger(__name__)


__author__ = 'jorgealegre'


class DatabaseFeatures(NonrelDatabaseFeatures):

    string_based_auto_field = True


class DatabaseOperations(NonrelDatabaseOperations):

    compiler_module = 'django_elasticsearch.compiler'
    SCROLL_TIME = '10m'
    ADD_BULK_SIZE = 1000

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

    def create_index(self, index_name, options=None, has_alias=True, model=None,
                     skip_register=False, index_settings=None):
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
        alias = index_name if has_alias is True else None
        index_name = u'{}-{}'.format(index_name, datetime.now().strftime("%Y.%m.%d"))
        es_connection = self.connection.connection
        if index_settings is None and options is not None:
            index_settings = {
                'analysis': options.get('ANALYSIS', {}),
                'number_of_replicas': options.get('NUMBER_OF_REPLICAS', NUMBER_OF_REPLICAS),
                'number_of_shards': options.get('NUMBER_OF_SHARDS', NUMBER_OF_SHARDS),
            }
        es_connection.indices.create_index(index_name, settings=index_settings)
        # alias
        if has_alias:
            es_connection.indices.add_alias(alias, index_name)
        if not skip_register:
            # save index creation data
            es_connection.index({
                'operation': OPERATION_CREATE_INDEX,
                'index_name': index_name,
                'alias': alias,
                'settings': index_settings,
                'created_on': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'updated_on': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, INTERNAL_INDEX, 'indices')
        if has_alias:
            logger.info(u'index "{}" aliased "{}" created'.format(index_name, alias))
        else:
            logger.info(u'index "{}" created'.format(index_name))
        return index_name, alias

    def delete_index(self, index_name, skip_register=False):
        """
        Deletes index

        :param index_name: Index name
        :return:
        """
        es_connection = self.connection.connection
        es_connection.indices.delete_index(index_name)
        # save index creation data
        if not skip_register:
            es_connection.index({
                'operation': OPERATION_DELETE_INDEX,
                'alias': index_name,
                'created_on': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                'updated_on': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }, INTERNAL_INDEX, 'indices')
        logger.info(u'index "{}" deleted'.format(index_name))

    def register_index_operation(self, index_name, operation, index_settings, model=None):
        """
        Register index operation

        :param index_name:
        :param operation:
        :return:
        """
        es_connection = self.connection.connection

        es_connection.index({
            'operation': operation,
            'index_name': u'{}-{}'.format(index_name, datetime.now().strftime("%Y.%m.%d")),
            'alias': index_name,
            'model': model or '',
            'settings': index_settings,
            'created_on': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'updated_on': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }, INTERNAL_INDEX, 'indices')
        logger.info(u'register_index_operation :: operation: {} index: {}'.format(
            operation,
            index_name,
        ))

    def register_mapping_update(self, index_name, mapping, mapping_old=''):
        """
        Register mapping update, writing sent mapping, current mapping at ES, and ES
        processed mapping after sent (returned by ES)

        :param index_name:
        :param mapping:
        :return:
        """
        import base64
        mapping_dict = mapping
        if not isinstance(mapping, dict):
            mapping_dict = mapping.as_dict()
        es_connection = self.connection.connection
        # TODO get last sequence, add by one and have format
        # '{0:05d}'.format(2)
        path = u'/{}/_mapping/{}/'.format(mapping.index_name, mapping.name)
        logger.debug(u'register_mapping_update :: path: {}'.format(path))
        result = es_connection._send_request('GET', path)
        logger.debug(u'register_mapping_update :: result: {}'.format(result))
        mapping_server = result[result.keys()[0]]['mappings']
        es_connection.index({
            'operation': OPERATION_UPDATE_MAPPING,
            'doc_type': mapping.name,
            'index_name': mapping.index_name,
            'sequence': '99999',
            'mapping': base64.encodestring(json.dumps(mapping_dict)),
            'mapping_old': mapping_old,
            'mapping_server': base64.encodestring(json.dumps(mapping_server)),
            'created_on': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'updated_on': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }, INTERNAL_INDEX, 'mapping_migration')
        logger.info(u'register_mapping_update :: index: {} doc_type: {}'.format(
            index_name,
            mapping.name,
        ))

    def get_mappings(self, index_name, doc_type):
        """
        Get mappings for index and doc_type in dict form

        :param index_name:
        :param doc_type:
        :return: dictionary with mapping on ElasticSearch
        """
        es_connection = self.connection.connection
        path = u'/{}/_mapping/{}/'.format(index_name, doc_type)
        result = es_connection._send_request('GET', path)
        mapping_dict = result[result.keys()[0]]['mappings']
        return mapping_dict

    def rebuild_index(self, alias):
        """
        Rebuilds index in the background

        1. Rebuild global index: Rebuilds whole database with all models
        2. Model rebuild: Rebuilds only model main store data and associated indexes

        :param index_name: Index name

        :return:
        """
        es_connection = self.connection.connection
        options = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('OPTIONS', {})
        # 1. create alt index
        index_data = self.create_index(alias, options, has_alias=False)
        index_name_physical = index_data[0]
        # 2. Inspect all models: create mappings for alt index: mapping.save()
        if alias in settings.DATABASES:
            # global index
            for app_name, app_models in self.connection.introspection.models.iteritems():
                for model in app_models:
                    mapping = model_to_mapping(model, es_connection, index_name_physical)
                    mapping.save()
        else:
            # get model by index
            query = '(alias:({alias}))'.format(alias)
            results = es_connection.search(Search(QueryStringQuery(query)),
                                           indices=INTERNAL_INDEX,
                                           doc_types='model')
            if results and len(results) == 1:
                model = results[0].model
                mapping = model_to_mapping(model, es_connection, index_name_physical)
                mapping.save()
            else:
                # raise exception RebuildIndexException
                raise exceptions.RebuildIndexException(_(u'No model data found in {}'.format(INTERNAL_INDEX)))
        # 2. export/import data to new index
        # bulk operations
        results = es_connection.search(Search(QueryStringQuery('*:*')),
                                       indices=alias,
                                       scroll=self.SCROLL_TIME)
        scroll_id = results.scroller_id
        es_connection.bulk_size = self.ADD_BULK_SIZE
        bulk = es_connection.create_bulker()
        while results:
            for result in results:
                # add to bulk for index
                # content = json.dumps(result.get_meta()) + '\n'
                meta = result.get_meta()
                content = '{ "index" : { "_index" : "{index_name}", "_type" : "{doc_type}", ' \
                          '"_id" : "{id}" } }\n'\
                    .format(index_name_physical,
                            meta['type'],
                            meta['id'])
                content += json.dumps(result) + '\n'
                bulk.add(content)
            # make bulk add to new index "index_name_physical"
            results = es_connection.search_scroll(scroll_id, scroll=self.SCROLL_TIME)
        bulk.flush_bulk()
        # 3. assign alias to new index
        indices = es_connection.indices.get_alias(alias)
        es_connection.indices.change_aliases([
            ('remove', indices[0], alias),
            ('add', index_name_physical, alias),
        ])
        # 4. delete old index
        self.delete_index(index_name_physical)

    def build_es_settings_from_django(self, options):
        """
        Build ElasticSearch settings from django options in DATABASES setting

        :param options:
        :return:
        """
        es_settings = {}
        es_settings.update({
            'number_of_replicas': options.get('NUMBER_OF_REPLICAS', 1),
            'number_of_shards': options.get('NUMBER_OF_SHARDS', 5),
        })
        if 'ANALYSIS' in options and options['ANALYSIS'].keys():
            es_settings['analysis'] = options.get('ANALYSIS', '')
        return es_settings

    def build_django_engine_structure(self):
        """
        Build and save .django_engine mappings for document types

        :return:
        """
        from django_elasticsearch.fields import DocumentObjectField, DateField, StringField, ObjectField, \
            IntegerField
        es_connection = self.connection.connection
        # create .django_engine index
        try:
            # build settings
            # attach mappings to settings
            options = {
                'number_of_replicas': 1,
                'number_of_shards': 1,
            }
            # index_settings = SettingsBuilder(options, mappings)
            self.create_index(INTERNAL_INDEX, options=options, skip_register=True)
            # indices
            mapping_indices = DocumentObjectField(
                name='indices',
                connection=self.connection,
                index_name=INTERNAL_INDEX,
                properties={
                    'operation': StringField(index='not_analyzed'),
                    'index_name': StringField(index='not_analyzed'),
                    'alias': StringField(index='not_analyzed'),
                    'model': StringField(index='not_analyzed'),
                    'settings': ObjectField(),
                    'created_on': DateField(),
                    'updated_on': DateField(),
                })
            result = es_connection.indices.put_mapping(doc_type='indices',
                                                       mapping=mapping_indices,
                                                       indices=INTERNAL_INDEX)
            logger.info(u'{} result: {}'.format('.django_engine/indices',
                                                pprint.PrettyPrinter(indent=4).pformat(result)))
            # mapping_migration
            mapping_migration = DocumentObjectField(
                name='mapping_migration',
                connection=self.connection,
                index_name=INTERNAL_INDEX,
                properties={
                    'operation': StringField(index='not_analyzed'),
                    'doc_type': StringField(index='not_analyzed'),
                    'index_name': StringField(index='not_analyzed'),
                    'sequence': IntegerField(),
                    'mapping': StringField(index='not_analyzed'),
                    'mapping_server': StringField(index='not_analyzed'),
                    'mapping_old': StringField(index='not_analyzed'),
                    'created_on': DateField(),
                    'updated_on': DateField(),
                })
            result = es_connection.indices.put_mapping(doc_type='mapping_migration',
                                                       mapping=mapping_migration,
                                                       indices=INTERNAL_INDEX)
            logger.info(u'{} result: {}'.format('.django_engine/mapping_migration',
                                                pprint.PrettyPrinter(indent=4).pformat(result)))
            # model
            mapping_model = DocumentObjectField(
                name='model',
                connection=self.connection,
                index_name=INTERNAL_INDEX,
                properties={
                    'index_name': StringField(index='not_analyzed'),
                    'alias': StringField(index='not_analyzed'),
                    'model': StringField(index='not_analyzed'),
                    'mapping': StringField(index='not_analyzed'),
                    'settings': ObjectField(),
                    'created_on': DateField(),
                    'updated_on': DateField(),
                })
            result = es_connection.indices.put_mapping(doc_type='model',
                                                       mapping=mapping_model,
                                                       indices=INTERNAL_INDEX)
            logger.info(u'{} result: {}'.format('.django_engine/model',
                                                pprint.PrettyPrinter(indent=4).pformat(result)))

            # register index operation
            self.register_index_operation(INTERNAL_INDEX, OPERATION_CREATE_INDEX, options)
            # register mapping update
            self.register_mapping_update(INTERNAL_INDEX, mapping_indices)
            self.register_mapping_update(INTERNAL_INDEX, mapping_migration)
            self.register_mapping_update(INTERNAL_INDEX, mapping_model)
        except (IndexAlreadyExistsException, ElasticSearchException):
            traceback.print_exc()
            logger.info(u'Could not create index')


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
        logger.debug(u'connect... es_url: {} options: {}'.format(self.es_url, self.settings_dict))
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
