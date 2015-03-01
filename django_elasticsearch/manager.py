from django.db import connections
from django.db.models.manager import Manager as DJManager
from django.db.models.fields import FieldDoesNotExist

from pyes.queryset import QuerySet
from pyes.models import ElasticSearchModel

__author__ = 'jorgealegre'


'''class Manager(DJManager):

    def __init__(self, manager_func=None):
        super(Manager, self).__init__()
        self._manager_func = manager_func
        self._collection = None

    def contribute_to_class(self, model, name):
        # TODO: Use weakref because of possible memory leak / circular reference.
        self.model = model
#        setattr(model, name, ManagerDescriptor(self))
        if model._meta.abstract or (self._inherited and not self.model._meta.proxy):
            model._meta.abstract_managers.append((self.creation_counter, name,
                    self))
        else:
            model._meta.concrete_managers.append((self.creation_counter, name,
                self))

    def __get__(self, instance, owner):
        """Descriptor for instantiating a new QuerySet object when
        Document.objects is accessed.
        """
        self.model = owner #We need to set the model to get the db

        if instance is not None:
            # Document class being used rather than a document object
            return self

        if self._collection is None:
            self._collection = connections[self.db].db_connection[owner._meta.db_table]

        # owner is the document that contains the QuerySetManager
        queryset = QuerySet(owner, self._collection)
        if self._manager_func:
            if self._manager_func.func_code.co_argcount == 1:
                queryset = self._manager_func(queryset)
            else:
                queryset = self._manager_func(owner, queryset)
        return queryset'''


class IndexManager(DJManager):

    def __init__(self):
        super(IndexManager, self).__init__()

    def get_queryset(self):
        # 1. get connection
        # 2. instantiate es.QuerySet
        MyModel = type('MyModel', (ElasticSearchModel,), {})
        connection = connections[self.db].db_connection
        return QuerySet(MyModel, index=index, type=doc_type, es_url=connection.es_url, es_kwargs=es_kwargs))


class ESMeta(object):
    pass


def add_es_manager(sender, **kwargs):
    """
    Fix autofield
    """
    from django.conf import settings

    cls = sender
    database = settings.DATABASES[cls.objects.db]
    if 'elasticsearch' in database['ENGINE']:
        if cls._meta.abstract:
            return

        if getattr(cls, 'objects', None) is None:
            # Create the default manager, if needed.
            try:
                cls._meta.get_field('objects')
                raise ValueError("Model %s must specify a custom Manager, because it has a field named "
                                 "'index'" % cls.__name__)
            except FieldDoesNotExist:
                pass
            setattr(cls, 'objects', IndexManager())
