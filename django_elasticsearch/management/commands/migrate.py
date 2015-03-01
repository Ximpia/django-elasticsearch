# python
import logging
import pprint
import sys
import traceback

# django
from django.conf import settings
from django.db import connections, DEFAULT_DB_ALIAS
from django.core.management.base import BaseCommand

# pyes
from pyes.exceptions import IndexAlreadyExistsException, IndexMissingException

# django_elasticsearch_Engine
from django_elasticsearch.mapping import model_to_mapping
from django_elasticsearch.models import get_settings_by_meta
from django_elasticsearch import ENGINE, NUMBER_OF_REPLICAS, NUMBER_OF_SHARDS, INTERNAL_INDEX
from django_elasticsearch.fields import DocumentObjectField, DateField, StringField, IntegerField, \
    ObjectField

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class Command(BaseCommand):

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
            self.connection.ops.build_django_engine_structure()
            try:
                self.connection.ops.create_index(global_index_name, options)
                self.stdout.write(u'index "{}" created'.format(global_index_name))
            except IndexAlreadyExistsException:
                pass
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
