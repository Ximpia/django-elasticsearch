import django.db.models.options as options

__author__ = 'jorgealegre'

options.DEFAULT_NAMES = options.DEFAULT_NAMES + ('indices',
                                                 'disable_default_index')


class DjangoElasticEngineException(Exception):
    pass
