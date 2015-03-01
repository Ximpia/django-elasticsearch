import logging

try:
    from django.db.backends.schema import BaseDatabaseSchemaEditor
except ImportError:
    BaseDatabaseSchemaEditor = object

logger = logging.getLogger(__name__)

__author__ = 'jorgealegre'


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    def create_model(self, model):
        logger.debug(u'DatabaseSchemaEditor :: create_model...')
        pass

    def delete_model(self, model):
        logger.debug(u'DatabaseSchemaEditor :: delete_model...')
        pass
