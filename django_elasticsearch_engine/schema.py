try:
    from django.db.backends.schema import BaseDatabaseSchemaEditor
except ImportError:
    BaseDatabaseSchemaEditor = object

__author__ = 'jorgealegre'


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    def create_model(self, model):
        pass

    def delete_model(self, model):
        pass
