# python
import logging

# django
from djangotoolbox.db.base import NonrelDatabaseCreation

# pyes
from pyes.exceptions import NotFoundException

# djes
from mapping import model_to_mapping

TEST_DATABASE_PREFIX = 'test_'

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class DatabaseCreation(NonrelDatabaseCreation):
    data_types = {
        'DateTimeField':                'date',
        'DateField':                    'date',
        'TimeField':                    'time',
        'FloatField':                   'float',
        'EmailField':                   'string',
        'URLField':                     'string',
        'BooleanField':                 'bool',
        'NullBooleanField':             'bool',
        'CharField':                    'string',
        'CommaSeparatedIntegerField':   'string',
        'IPAddressField':               'ip',
        'SlugField':                    'string',
        'FileField':                    'string',
        'FilePathField':                'string',
        'TextField':                    'string',
        'XMLField':                     'string',
        'IntegerField':                 'integer',
        'SmallIntegerField':            'integer',
        'PositiveIntegerField':         'integer',
        'PositiveSmallIntegerField':    'integer',
        'BigIntegerField':              'long',
        'GenericAutoField':             'string',
        'StringForeignKey':             'string',
        'AutoField':                    'string',
        'RelatedAutoField':             'string',
        'OneToOneField':                'string',
        'DecimalField':                 'decimal',
        'AbstractIterableField':        'nested',
        'ListField':                    'nested',
        'SetField':                     'nested',
        'DictField':                    'object',
        'EmbeddedModelField':           'object',
    }

    def sql_indexes_for_field(self, model, f, style):
        return []

    def index_fields_group(self, model, group, style):
        return []

    def sql_indexes_for_model(self, model, style):
        print 'sql_indexes_for_model....'
        return []

    def sql_create_model(self, model, style, known_models=set()):
        """
        Create mapping for model

        :param model
        :param style
        :param known_models
        :rtype list, dict
        """
        logger.debug(u'sql_create_model....')
        logger.debug(u'index: {}'.format(model._meta.db_table))
        self.connection.put_mapping(model._meta.db_table, model_to_mapping(model).as_dict())
        return [], {}

    def create_test_db(self, verbosity=1, autoclobber=False):
        """
        """
        from django.core.management import call_command

        test_database_name = self._get_test_db_name()
        self.connection.settings_dict['NAME'] = test_database_name

        if verbosity >= 1:
            print("Creating test database for alias '{}'".format(self.connection.alias))

        try:
            self._drop_database(test_database_name)
        except NotFoundException:
            pass

        self.connection.indices.create_index(test_database_name)
        self.connection.cluster.cluster_health(wait_for_status='green')

        call_command('migrate',
                     verbosity=max(verbosity - 1, 0),
                     interactive=False,
                     database=self.connection.alias,
                     load_initial_data=False)

    def destroy_test_db(self, old_database_name, verbosity=1):
        """
        Destroy a test database, prompting the user for confirmation if the
        database already exists. Returns the name of the test database created.
        """
        if verbosity >= 1:
            print "Destroying test database '%s'..." % self.connection.alias
        test_database_name = self.connection.settings_dict['NAME']
        self._drop_database(test_database_name)
        self.connection.settings_dict['NAME'] = old_database_name

    def _drop_database(self, database_name):
        try:
            self.connection.indices.delete_index(database_name)
        except NotFoundException:
            pass
        self.connection.cluster.cluster_health(wait_for_status='green')

    def sql_destroy_model(self, model, references_to_delete, style):
        print model
