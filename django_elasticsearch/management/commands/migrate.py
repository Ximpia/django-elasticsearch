# python
import logging

# django
from django.conf import settings
from django.db import connections, DEFAULT_DB_ALIAS
from django.core.management.base import BaseCommand

# pyes
from pyes.exceptions import IndexAlreadyExistsException, ElasticSearchException

# django_elasticsearch
from django_elasticsearch.mapping import model_to_mapping
from django_elasticsearch.models import get_settings_by_meta
from django_elasticsearch import ENGINE, OPERATION_CREATE_INDEX

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):
        engine = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('ENGINE', '')
        global_index_name = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('NAME', '')
        options = settings.DATABASES.get(DEFAULT_DB_ALIAS, {}).get('OPTIONS', {})
        connection = connections[DEFAULT_DB_ALIAS]
        es_connection = connection.connection

        # Call regular migrate if engine is different from ours
        if engine != ENGINE:
            return super(Command, self).handle(**options)
        else:
            # project global index
            has_alias = connection.ops.has_alias(global_index_name)
            if not has_alias:
                try:
                    index_name_final, alias = connection.ops.create_index(global_index_name, options,
                                                                          skip_register=True)
                    self.stdout.write(u'index "{}" created with physical name "{}"'.format(alias, index_name_final))
                    connection.ops.build_django_engine_structure()
                    # register create index for global
                    connection.ops.register_index_operation(index_name_final, OPERATION_CREATE_INDEX,
                                                            connection.ops.build_es_settings_from_django(options))
                except IndexAlreadyExistsException:
                    pass
                except ElasticSearchException:
                    import traceback
                    logger.error(traceback.format_exc())

            logger.debug(u'models: {}'.format(connection.introspection.models))
            for app_name, app_models in connection.introspection.models.iteritems():
                for model in app_models:
                    mapping = model_to_mapping(model, es_connection, global_index_name)
                    try:
                        mapping.save()
                        self.stdout.write(u'Mapping for model {}.{} updated'.format(app_name, model.__name__))
                    except Exception as e:
                        import traceback
                        logger.error(traceback.format_exc())
                        self.stderr.write(u'Could not update mapping, rebuilding global index...')
                        connection.ops.rebuild_index(global_index_name)
                        mapping.save()
                    if not hasattr(model._meta, 'indices'):
                        continue
                    for model_index in model._meta.indices:
                        model_index_name = model_index.keys()[0]
                        index_name = u'{}__{}'.format(model._meta.db_table, model_index_name)
                        logger.debug(u'model index name: {}'.format(index_name))
                        index_data = model_index[model_index_name]
                        logger.debug(u'index_data: {}'.format(index_data))
                        try:
                            index_physical, alias = connection.ops.create_index(index_name,
                                                                                get_settings_by_meta(index_data))
                            self.stdout.write(u'index "{}" created with physical name "{}"'.format(alias,
                                                                                                   index_physical))
                        except IndexAlreadyExistsException:
                            pass
                        mapping = model_to_mapping(model, es_connection, index_name)
                        try:
                            mapping.save()
                            self.stdout.write(u'Mapping for model {}.{} updated'
                                              .format(app_name, index_name))
                        except Exception as e:
                            self.stderr.write(u'Could not update mapping, rebuilding index "{}" ...'
                                              .format(index_name))
                            connection.ops.rebuild_index(index_name)
                            mapping.save()
