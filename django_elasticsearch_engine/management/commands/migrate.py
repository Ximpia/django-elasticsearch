# python
import logging
import pprint

# django
from django.conf import settings
# from django.core.management.commands import syncdb
from django.db import connections
from django.core.management.base import BaseCommand

# pyes
from pyes.exceptions import IndexAlreadyExistsException, IndexMissingException

# django_elasticsearch_Engine
from django_elasticsearch_engine.mapping import model_to_mapping
from django_elasticsearch_engine import ENGINE

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):
        databases = settings.DATABASES.keys()
        for db in databases:
            engine = settings.DATABASES.get(db, {}).get('ENGINE', '')
            index_name = settings.DATABASES.get(db, {}).get('NAME', '')
            logger.debug(u'db: {} engine: {} index_name: {}'.format(db, engine, index_name))

            # Call regular migrate if engine is different from ours
            if engine != ENGINE:
                return super(Command, self).handle(**options)
            else:
                connection = connections[db]
                es_connection = connection.connection
                try:
                    logger.debug(u'indices: {}'.format(es_connection.indices.get_indices()))
                except IndexMissingException:
                    pass
                try:
                    # TODO create with settings from database options
                    es_connection.indices.create_index(index_name)
                    self.stdout.write(u'Created index "{}"'.format(index_name))
                except IndexAlreadyExistsException:
                    self.stderr.write(u'Index "{}" already exists!!!'.format(index_name))
                    ans = raw_input(u'\n** WARNING **\nI will remove index "{}". Do you want to continue? (y/n) <y> '
                                    .format(index_name))
                    ans = ans or 'y'
                    if ans.lower() == 'n':
                        continue
                    else:
                        es_connection.indices.delete_index(index_name)
                        self.stdout.write(u'\nindex "{}" removed'.format(index_name))
                        es_connection.indices.create_index(index_name)
                        self.stdout.write(u'index "{}" created'.format(index_name))
                logger.debug(u'models: {}'.format(connection.introspection.models))
                for app_name, app_models in connection.introspection.models.iteritems():
                    for model in app_models:
                        self.stdout.write(u'Mappings %s.%s' % (app_name, model.__name__))
                        mapping = model_to_mapping(model).as_dict()
                        logger.debug(u'mapping: {}'.format(pprint.PrettyPrinter(indent=4).pformat(mapping)))
                        logger.debug(u'doc_type: {}'.format(model._meta.db_table))
                        result = es_connection.indices.put_mapping(
                            model._meta.db_table,
                            mapping,
                            indices=[index_name]
                        )
                        logger.info(u'result: {}'.format(result))
