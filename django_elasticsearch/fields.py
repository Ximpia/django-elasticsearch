# python
import logging
import pprint
import json
from datetime import datetime

# django

# pyes
from pyes import mappings
from pyes.query import QueryStringQuery, Search

# django_elasticsearch
from django_elasticsearch import INTERNAL_INDEX

from django.db import models

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class StringField(mappings.StringField):

    def __init__(self, *args, **kwargs):
        super(StringField, self).__init__(*args, **kwargs)

    def as_dict(self):
        map_ = super(StringField, self).as_dict()
        if self.index is False and self.tokenize is False:
            map_['index'] = 'no'
        elif self.index is True and self.tokenize is False:
            map_['index'] = 'not_analyzed'
        elif self.index is True and self.tokenize is True:
            map_['index'] = 'analyzed'
        return map_


class DateField(mappings.DateField):

    def __init__(self, *args, **kwargs):
        super(DateField, self).__init__(*args, **kwargs)


class BooleanField(mappings.BooleanField):

    def __init__(self, *args, **kwargs):
        super(BooleanField, self).__init__(*args, **kwargs)


class DoubleField(mappings.DoubleField):

    def __init__(self, *args, **kwargs):
        super(DoubleField, self).__init__(*args, **kwargs)


class FloatField(mappings.FloatField):

    def __init__(self, *args, **kwargs):
        super(FloatField, self).__init__(*args, **kwargs)


class IntegerField(mappings.IntegerField):

    def __init__(self, *args, **kwargs):
        super(IntegerField, self).__init__(*args, **kwargs)


class LongField(mappings.LongField):

    def __init__(self, *args, **kwargs):
        super(LongField, self).__init__(*args, **kwargs)


class MultiField(mappings.MultiField):

    def __init__(self, *args, **kwargs):
        super(MultiField, self).__init__(*args, **kwargs)


class NestedObject(mappings.NestedObject):

    def __init__(self, *args, **kwargs):
        super(NestedObject, self).__init__(*args, **kwargs)


class ShortField(mappings.ShortField):

    def __init__(self, *args, **kwargs):
        super(ShortField, self).__init__(*args, **kwargs)


class ObjectField(mappings.ObjectField):

    def __init__(self, *args, **kwargs):
        super(ObjectField, self).__init__(*args, **kwargs)

    def as_dict(self):
        map_ = super(ObjectField, self).as_dict()
        del map_['type']
        return map_


class DocumentObjectField(mappings.DocumentObjectField):

    def __init__(self, *args, **kwargs):
        super(DocumentObjectField, self).__init__(*args, **kwargs)

    def as_dict(self):
        map_ = super(DocumentObjectField, self).as_dict()
        del map_['type']
        return map_

    def save(self):
        if self.connection is None:
            raise RuntimeError(u"No connection available")
        try:
            mappings_old = self.connection.indices.get_mapping(doc_type=self.name,
                                                               indices=self.index_name)
            result = self.connection.indices.put_mapping(doc_type=self.name,
                                                         mapping=self.as_dict(),
                                                         indices=self.index_name)
            logger.info(u'result: {}'.format(pprint.PrettyPrinter(indent=4).pformat(result)))
            # update internal .django_engine index
            self.connection.index({
                'operation': 'put_mapping',
                'doc_type': self.name,
                'mapping_old': json.dumps(mappings_old.as_dict()),
                'mapping_new':  json.dumps(result.as_dict()),
                'created_on': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'updated_on': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, INTERNAL_INDEX, 'mapping_migration')
        except Exception:
            # reindex
            # MergeMappingException
            # 1. create alt index
            # 2. export/import data to new index
            # 3. assign alias to new index
            # 4. delete old index
            # TODO: Implement model reindex
            logger.info(u'Could not update mappings for doc_type:"{}"'.format(self.name))
