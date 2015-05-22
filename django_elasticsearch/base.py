# python
from itertools import chain
import logging
import traceback
import pprint
from datetime import datetime
import json
import pickle

# django
from django.db.backends import connection_created
from django.db import connections, router, transaction, models as dj_models, DEFAULT_DB_ALIAS
from django.utils.datastructures import SortedDict
from django.conf import settings
from django.utils.translation import ugettext as _
from django.utils.functional import Promise
from django.utils.safestring import EscapeString, EscapeUnicode, SafeString, \
    SafeUnicode
from django.db.models.fields.related import ManyToManyField


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

    def value_for_db(self, value, field, lookup=None):
        """
        Does type-conversions needed before storing a value in the
        the database or using it as a filter parameter.

        This is a convience wrapper that only precomputes field's kind
        and a db_type for the field (or the primary key of the related
        model for ForeignKeys etc.) and knows that arguments to the
        `isnull` lookup (`True` or `False`) should not be converted,
        while some other lookups take a list of arguments.
        In the end, it calls `_value_for_db` to do the real work; you
        should typically extend that method, but only call this one.

        :param value: A value to be passed to the database driver
        :param field: A field the value comes from
        :param lookup: None if the value is being prepared for storage;
                       lookup type name, when its going to be used as a
                       filter argument
        """
        field, field_kind, db_type = self._convert_as(field, lookup)

        # Argument to the "isnull" lookup is just a boolean, while some
        # other lookups take a list of values.
        if lookup == 'isnull':
            return value
        elif lookup in ('in', 'range', 'year'):
            return [self._value_for_db(subvalue, field,
                                       field_kind, db_type, lookup)
                    for subvalue in value]
        else:
            return self._value_for_db(value, field,
                                      field_kind, db_type, lookup)

    def _value_for_db(self, value, field, field_kind, db_type, lookup):
        """
        Converts a standard Python value to a type that can be stored
        or processed by the database driver.

        This implementation only converts elements of iterables passed
        by collection fields, evaluates Django's lazy objects and
        marked strings and handles embedded models.
        Currently, we assume that dict keys and column, model, module
        names (strings) of embedded models require no conversion.

        We need to know the field for two reasons:
        -- to allow back-ends having separate key spaces for different
           tables to create keys refering to the right table (which can
           be the field model's table or the table of the model of the
           instance a ForeignKey or other relation field points to).
        -- to know the field of values passed by typed collection
           fields and to use the proper fields when deconverting values
           stored for typed embedding field.
        Avoid using the field in any other way than by inspecting its
        properties, it may not hold any value or hold a value other
        than the one you're asked to convert.

        You may want to call this method before doing other back-end
        specific conversions.

        :param value: A value to be passed to the database driver
        :param field: A field having the same properties as the field
                      the value comes from; instead of related fields
                      you'll get the related model primary key, as the
                      value usually needs to be converted using its
                      properties
        :param field_kind: Equal to field.get_internal_type()
        :param db_type: Same as creation.db_type(field)
        :param lookup: None if the value is being prepared for storage;
                       lookup type name, when its going to be used as a
                       filter argument
        """
        # Back-ends may want to store empty lists or dicts as None.
        if value is None:
            return None

        # Force evaluation of lazy objects (e.g. lazy translation
        # strings).
        # Some back-ends pass values directly to the database driver,
        # which may fail if it relies on type inspection and gets a
        # functional proxy.
        # This code relies on unicode cast in django.utils.functional
        # just evaluating the wrapped function and doing nothing more.
        # TODO: This has been partially fixed in vanilla with:
        #       https://code.djangoproject.com/changeset/17698, however
        #       still fails for proxies in lookups; reconsider in 1.4.
        #       Also research cases of database operations not done
        #       through the sql.Query.
        if isinstance(value, Promise):
            value = unicode(value)

        # Django wraps strings marked as safe or needed escaping,
        # convert them to just strings for type-inspecting back-ends.
        if isinstance(value, (SafeString, EscapeString)):
            value = str(value)
        elif isinstance(value, (SafeUnicode, EscapeUnicode)):
            value = unicode(value)

        # Convert elements of collection fields.
        # We would need to test set and list collections. DictField should do OK with ObjectField
        if field_kind in ('ListField', 'SetField', 'DictField',):
            value = self._value_for_db_collection(value, field,
                                                  field_kind, db_type, lookup)
        return value

    def to_dict(self, instance):
        opts = instance._meta
        data = {}
        for f in opts.concrete_fields + opts.many_to_many:
            if isinstance(f, ManyToManyField):
                if instance.pk is None:
                    data[f.name] = []
                else:
                    data[f.name] = list(f.value_from_object(instance).values_list('pk', flat=True))
            else:
                data[f.name] = f.value_from_object(instance)
        return data

    def _value_for_db_model(self, value, field, field_kind, db_type, lookup):
        """
        Converts a field => value mapping received from an
        EmbeddedModelField the format chosen for the field storage.

        The embedded instance fields' values are also converted /
        deconverted using value_for/from_db, so any back-end
        conversions will be applied.

        Returns (field.column, value) pairs, possibly augmented with
        model info (to be able to deconvert the embedded instance for
        untyped fields) encoded according to the db_type chosen.
        If "dict" db_type is given a Python dict is returned.
        If "list db_type is chosen a list with columns and values
        interleaved will be returned. Note that just a single level of
        the list is flattened, so it still may be nested -- when the
        embedded instance holds other embedded models or collections).
        Using "bytes" or "string" pickles the mapping using pickle
        protocol 0 or 2 respectively.
        If an unknown db_type is used a generator yielding (column,
        value) pairs with values converted will be returned.

        TODO: How should EmbeddedModelField lookups work?
        """
        # value would by id or list of ids for many relationships
        if lookup:
            # raise NotImplementedError("Needs specification.")
            return value

        # Convert using proper instance field's info, change keys from
        # fields to columns.
        # TODO/XXX: Arguments order due to Python 2.5 compatibility.
        value = (
            (subfield.column, self._value_for_db(
                subvalue, lookup=lookup, *self._convert_as(subfield, lookup)))
            for subfield, subvalue in value.iteritems())

        # Cast to a dict, interleave columns with values on a list,
        # serialize, or return a generator.
        if db_type == 'dict':
            value = dict(value)
        elif db_type == 'list':
            value = list(item for pair in value for item in pair)
        elif db_type == 'bytes':
            value = pickle.dumps(dict(value), protocol=2)
        elif db_type == 'string':
            value = pickle.dumps(dict(value))

        return value

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        for table in tables:
            self.connection.indices.delete_mapping(self.connection.db_name, table)
        return []

    def _convert_as(self, field, lookup=None):
        """
        Computes parameters that should be used for preparing the field
        for the database or deconverting a database value for it.
        """
        # We need to compute db_type using the original field to allow
        # GAE to use different storage for primary and foreign keys.
        db_type = self.connection.creation.db_type(field)

        if field.rel is not None:
            field = field.rel.get_related_field()
        field_kind = field.get_internal_type()

        # Values for standard month / day queries are integers.
        if (field_kind in ('DateField', 'DateTimeField') and
                lookup in ('month', 'day')):
            db_type = 'integer'

        return field, field_kind, db_type

    def convert_as(self, field, lookup=None):
        """
        Get field data

        :param field:
        :param lookup:
        :return:
        """
        return self._convert_as(field, lookup)

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
        import random
        alias = index_name if has_alias is True else None
        index_name = u'{}-{}_{}'.format(
            index_name,
            datetime.now().strftime("%Y.%m.%d"),
            random.randint(1, 999)
        )
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
            self.register_index_operation(index_name, OPERATION_CREATE_INDEX, index_settings)
        if has_alias:
            logger.info(u'index "{}" aliased "{}" created'.format(index_name, alias))
        else:
            logger.info(u'index "{}" created'.format(index_name))
        return index_name, alias

    def has_alias(self, alias):
        """
        Check if alias exists

        :param alias:
        :return:
        """
        es_connection = self.connection.connection
        try:
            indices = es_connection.indices.get_alias(alias)
        except IndexMissingException:
            return False
        if indices:
            return True
        return False

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
            'index_name': index_name,
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
        # logger.debug(u'register_mapping_update :: path: {}'.format(path))
        result = es_connection._send_request('GET', path)
        # logger.debug(u'register_mapping_update :: result: {}'.format(result))
        mapping_server = result[result.keys()[0]]['mappings']
        if isinstance(mapping_old, dict):
            mapping_old = base64.encodestring(json.dumps(mapping_dict))
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
        try:
            mapping_dict = result[result.keys()[0]]['mappings']
        except IndexError:
            mapping_dict = {}
        return mapping_dict

    def rebuild_index(self, alias):
        """
        Rebuilds index in the background

        1. Rebuild global index: Rebuilds whole database with all models
        2. Model rebuild: Rebuilds only model main store data and associated indexes

        :param alias: Index alias

        :return:
        """
        es_connection = self.connection.connection
        options = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('OPTIONS', {})
        # 1. create alt index
        logger.debug(u'rebuild_index :: alias: {}'.format(alias))
        index_data = self.create_index(alias, options, has_alias=False)
        index_name_physical = index_data[0]
        # 2. Inspect all models: create mappings for alt index: mapping.save()
        if alias in map(lambda x: x['NAME'], settings.DATABASES.values()):
            # global index
            for app_name, app_models in self.connection.introspection.models.iteritems():
                for model in app_models:
                    mapping = model_to_mapping(model, es_connection, index_name_physical)
                    mapping.save()
        else:
            # get model by index
            # {model}__{model_index_name}
            if '__' not in alias:
                raise exceptions.RebuildIndexException(_(u'Invalid model index format "{}"'.format(alias)))
            alias_fields = alias.split('__')
            for app_name, app_models in self.connection.introspection.models.iteritems():
                for model in app_models:
                    if model._meta.db_table == alias_fields[0]:
                        mapping = model_to_mapping(alias_fields[0], es_connection, index_name_physical)
                        mapping.save()
        logger.debug(u'rebuild_index :: Updated mappings!!')
        # 2. export/import data to new index
        # bulk operations
        results = es_connection.search(Search(QueryStringQuery('*:*')),
                                       indices=es_connection.indices.get_alias(alias),
                                       scroll=self.SCROLL_TIME)
        scroll_id = results.scroller_id
        es_connection.bulk_size = self.ADD_BULK_SIZE
        bulk = es_connection.create_bulker()
        while results:
            logger.debug(u'rebuild_index :: results: {}'.format(results))
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
                # make bulk add to new index "index_name_physical"
                bulk.add(content)
            results = es_connection.search_scroll(scroll_id, scroll=self.SCROLL_TIME)
        bulk.flush_bulk()
        # 3. assign alias to new index
        indices = es_connection.indices.get_alias(alias)
        es_connection.indices.change_aliases([
            ('remove', indices[0], alias, {}),
            ('add', index_name_physical, alias, {}),
        ])
        # 4. delete old index
        self.delete_index(indices[0])

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
            # register index operation
            self.register_index_operation(INTERNAL_INDEX, OPERATION_CREATE_INDEX, options)
            # register mapping update
            self.register_mapping_update(INTERNAL_INDEX, mapping_indices)
            self.register_mapping_update(INTERNAL_INDEX, mapping_migration)
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

        all_models = list(chain.from_iterable(self.cql_models.values()))
        tables = [model.column_family_name(include_keyspace=False)
                  for model in all_models]
        return tables

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
        import pprint
        logger.debug(u'connect... es_url: {} options: {}'.format(self.es_url,
                                                                 pprint.PrettyPrinter(indent=4)
                                                                 .pformat(self.settings_dict)))
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
