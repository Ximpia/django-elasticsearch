from django.conf import settings
from django.core.management.commands import syncdb
from django.db import connections

from pyes.exceptions import IndexAlreadyExistsException

__author__ = 'jorgealegre'


class Command(syncdb):

    def handle(self, *args, **options):
        db = options.get('database')
        engine = settings.DATABASES.get(db, {}).get('ENGINE', '')
        index_name = settings.DATABASES.get(db, {}).get('NAME', '')

        # Call regular migrate if engine is different from ours
        if engine != 'django_elasticearch_engine':
            return super(Command, self).handle(**options)
        else:
            connection = connections[db]
            try:
                # TODO: Define settings from database options
                connection.indices.create_index(index_name)
                self.stdout.write(u'Created index {}'.format(index_name))
            except IndexAlreadyExistsException:
                pass
            connection.connect()
            for app_name, app_models in connection.introspection.cql_models.iteritems():
                for model in app_models:
                    self.stdout.write('Mappings %s.%s' % (app_name, model.__name__))
                    # TODO Create mappings
