# python
import logging
import pprint
import sys

# django
from django.conf import settings
from django.db import connections, DEFAULT_DB_ALIAS
from django.core.management.base import BaseCommand

# pyes
from pyes.exceptions import IndexAlreadyExistsException, IndexMissingException

# django_elasticsearch_Engine
from django_elasticsearch_engine.mapping import model_to_mapping
from django_elasticsearch_engine.models import get_settings_by_meta
from django_elasticsearch_engine import ENGINE, NUMBER_OF_REPLICAS, NUMBER_OF_SHARDS, INTERNAL_INDEX
from django_elasticsearch_engine.fields import DocumentObjectField, DateField, StringField, IntegerField, \
    ObjectField

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def _build_django_engine_structure(self):
        # crete .django_engine index
        self._create_index(INTERNAL_INDEX, {
            'number_of_replicas': 1,
            'number_of_shards': 5,
        })
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
            pass
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
            pass

    def _create_index(self, index_name, options):
        """

        :param index_name:
        :param options:
        :return:
        """
        es_connection = self.connection.connection
        try:
            es_connection.indices.create_index(index_name, settings={
                'analysis': options.get('ANALYSIS', {}),
                'number_of_replicas': options.get('NUMBER_OF_REPLICAS', NUMBER_OF_REPLICAS),
                'number_of_shards': options.get('NUMBER_OF_SHARDS', NUMBER_OF_SHARDS),
            })
            # Write pyes action
            self.stdout.write(u'index "{}" created'.format(index_name))
        except IndexAlreadyExistsException:
            pass

    def handle(self, *args, **options):
        engine = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('ENGINE', '')
        global_index_name = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('NAME', '')
        options = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('OPTIONS', {})
        self.options = options

        connection = connections[DEFAULT_DB_ALIAS]
        self.connection = connection
        es_connection = connection.connection

        # Call regular migrate if engine is different from ours
        if engine != ENGINE:
            return super(Command, self).handle(**options)
        else:
            self._build_django_engine_structure()
            self._create_index(global_index_name, options)
            logger.debug(u'models: {}'.format(connection.introspection.models))
            for app_name, app_models in connection.introspection.models.iteritems():
                for model in app_models:
                    self.stdout.write(u'Mappings {}.{}'.format(app_name, model.__name__))
                    mapping = model_to_mapping(model, es_connection, global_index_name)
                    logger.debug(u'mapping: {}'.format(pprint.PrettyPrinter(indent=4).pformat(mapping.as_dict())))
                    mapping.save()
                    if not hasattr(model._meta, 'indices'):
                        continue
                    for model_index in model._meta.indices:
                        index_name = u'{}__{}'.format(model._meta.db_table, model_index.keys()[0])
                        index_data = model_index[index_name]
                        self._create_index(es_connection, index_name, get_settings_by_meta(index_data))
                        self.stdout.write(u'index "{}" created'.format(index_name))
                        # build mapping based on index_data
                        if 'routing_field' in index_data:
                            mapping = model_to_mapping(model, es_connection, index_name, _routing={
                                'required': True,
                                'path': index_data['routing_field']
                            })
                        else:
                            mapping = model_to_mapping(model, es_connection, index_name)
                        logger.debug(u'mapping: {}'.format(pprint.PrettyPrinter(indent=4).pformat(mapping.as_dict())))
                        mapping.save()
