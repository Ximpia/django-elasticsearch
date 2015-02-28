import inspect

import django
# from django.db import connections, router, transaction, models, DEFAULT_DB_ALIAS
# from django.db import models as dj_models, router
from django.db import connections, router, transaction, models as dj_models, DEFAULT_DB_ALIAS
import django.db.models.options as options
from django.utils.datastructures import SortedDict


__author__ = 'jorgealegre'


options.DEFAULT_NAMES = options.DEFAULT_NAMES + ('indices',
                                                 'disable_default_index')

ENGINE = 'django_elasticsearch_engine'
NUMBER_OF_REPLICAS = 1
NUMBER_OF_SHARDS = 5
INTERNAL_INDEX = '.django_engine'


def get_installed_apps():
    """
    Return list of all installed apps
    """

    if django.VERSION >= (1, 7):
        from django.apps import apps
        return apps.get_apps()
    else:
        from django.db import models
        return models.get_apps()


class DjangoElasticEngineException(Exception):
    pass
