from django.db.backends import connection_created
from djangotoolbox.db.base import (
    NonrelDatabaseClient,
    NonrelDatabaseFeatures,
    NonrelDatabaseIntrospection,
    NonrelDatabaseOperations,
    NonrelDatabaseValidation,
    NonrelDatabaseWrapper,
    NonrelDatabaseCreation
)

try:
    from django.db.backends.schema import BaseDatabaseSchemaEditor
except ImportError:
    BaseDatabaseSchemaEditor = object

import pyes


__author__ = 'jorgealegre'


class DatabaseFeatures(NonrelDatabaseFeatures):

    string_based_auto_field = True


class DatabaseOperations(NonrelDatabaseOperations):

    # compiler_module = __name__.rsplit('.', 1)[0] + '.compiler'

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


class DatabaseClient(NonrelDatabaseClient):
    pass


class DatabaseValidation(NonrelDatabaseValidation):
    pass


class DatabaseIntrospection(NonrelDatabaseIntrospection):

    def table_names(self, cursor=None):
        """
        Show defined models
        """
        # TODO: get indices
        return []

    def sequence_list(self):
        # TODO: check if it's necessary to implement that
        pass


class DatabaseCreation(NonrelDatabaseCreation):
    pass


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    def create_model(self, model):
        pass

    def delete_model(self, model):
        pass


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

        del self.connection

    def connect(self):
        if not self.connected or self.connection is None:
            self.connection = pyes.ES('{}:{}'.format(self.settings_dict['HOST'], self.settings_dict['PORT']),
                                      default_indices=[self.settings_dict['NAME']])
            connection_created.send(sender=self.__class__, connection=self)
            # TODO: Define where to crete main index syncdb elastic_syncdb or similar????
            '''try:
                self.connection.indices.create_index(self.settings_dict['NAME'])
            except:
                pass'''
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

    def close(self):
        pass

    def _commit(self):
        pass

    def _rollback(self):
        pass

    def schema_editor(self, *args, **kwargs):
        """
        Returns a new instance of this backend's SchemaEditor (Django>=1.7)
        """
        return DatabaseSchemaEditor(self, *args, **kwargs)