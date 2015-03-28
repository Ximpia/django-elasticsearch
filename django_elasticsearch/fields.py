# python
import logging
import pprint
import json
from datetime import datetime

# django
from collections import OrderedDict
from django.db import connections, DEFAULT_DB_ALIAS

# pyes
from pyes import mappings
from pyes.query import QueryStringQuery, Search
from pyes.mappings import get_field
from pyes.models import DotDict, SortedDict

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

    def as_dict(self):
        map_ = super(DateField, self).as_dict()
        if self.index is False and self.tokenize is False:
            map_['index'] = 'no'
        elif self.index is True and self.tokenize is False:
            map_['index'] = 'not_analyzed'
        elif self.index is True and self.tokenize is True:
            map_['index'] = 'analyzed'
        return map_


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

    def as_dict(self):
        map_ = super(IntegerField, self).as_dict()
        if self.index is False and self.tokenize is False:
            map_['index'] = 'no'
        elif self.index is True and self.tokenize is False:
            map_['index'] = 'not_analyzed'
        elif self.index is True and self.tokenize is True:
            map_['index'] = 'analyzed'
        return map_


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

    def __init__(self, name=None, path=None, properties=None,
                 dynamic=None, enabled=None, include_in_all=None, dynamic_templates=None,
                 include_in_parent=None, include_in_root=None,
                 connection=None, index_name=None):
        self.name = name
        self.type = "object"
        self.path = path
        self.properties = properties
        self.include_in_all = include_in_all
        self.dynamic = dynamic
        self.dynamic_templates = dynamic_templates or []
        self.enabled = enabled
        self.include_in_all = include_in_all
        self.include_in_parent = include_in_parent
        self.include_in_root = include_in_root
        self.connection = connection
        self.index_name = index_name
        if properties:
            # name -> Field
            map_ = {}
            for item in properties:
                logger.debug(u'type: {}'.format(type(properties[item])))
                if isinstance(properties[item], dict):
                    logger.debug(u'Will get field from dictionary')
                    map_[item] = get_field(item, properties[item])
                else:
                    instance = properties[item]
                    instance.name = item
                    map_[item] = instance
            self.properties = OrderedDict(sorted([(name, data) for name, data in map_.items()]))
        else:
            self.properties = {}

    def as_dict(self):
        map_ = super(ObjectField, self).as_dict()
        if 'type' in map_:
            del map_['type']
        return map_


class DocumentObjectField(ObjectField):

    def __init__(self, _all=None, _boost=None, _id=None,
                 _index=None, _source=None, _type=None, _routing=None, _ttl=None,
                 _parent=None, _timestamp=None, _analyzer=None, _size=None, date_detection=None,
                 numeric_detection=None, dynamic_date_formats=None, _meta=None, *args, **kwargs):
        super(DocumentObjectField, self).__init__(*args, **kwargs)
        self._timestamp = _timestamp
        self._all = _all
        self._boost = _boost
        self._id = _id
        self._index = _index
        self._source = _source
        self._routing = _routing
        self._ttl = _ttl
        self._analyzer = _analyzer
        self._size = _size

        self._type = _type
        if self._type is None:
            self._type = {"store": "yes"}

        self._parent = _parent
        self.date_detection = date_detection
        self.numeric_detection = numeric_detection
        self.dynamic_date_formats = dynamic_date_formats
        self._meta = DotDict(_meta or {})

    def get_meta(self, subtype=None):
        """
        Return the meta data.
        """
        if subtype:
            return DotDict(self._meta.get(subtype, {}))
        return  self._meta

    def enable_compression(self, threshold="5kb"):
        self._source.update({"compress": True, "compression_threshold": threshold})

    def as_dict(self):
        result = super(DocumentObjectField, self).as_dict()
        result['_type'] = self._type
        if self._all is not None:
            result['_all'] = self._all
        if self._source is not None:
            result['_source'] = self._source
        if self._boost is not None:
            result['_boost'] = self._boost
        if self._routing is not None:
            result['_routing'] = self._routing
        if self._ttl is not None:
            result['_ttl'] = self._ttl
        if self._id is not None:
            result['_id'] = self._id
        if self._timestamp is not None:
            result['_timestamp'] = self._timestamp
        if self._index is not None:
            result['_index'] = self._index
        if self._parent is not None:
            result['_parent'] = self._parent
        if self._analyzer is not None:
            result['_analyzer'] = self._analyzer
        if self._size is not None:
            result['_size'] = self._size

        if self.date_detection is not None:
            result['date_detection'] = self.date_detection
        if self.numeric_detection is not None:
            result['numeric_detection'] = self.numeric_detection
        if self.dynamic_date_formats is not None:
            result['dynamic_date_formats'] = self.dynamic_date_formats
        if 'type' in result:
            del result['type']
        return result

    def save(self):
        """
        Save mapping, registering into .django_engine internal index

        :return:
        """
        if self.connection is None:
            raise RuntimeError(u"No connection available")
        try:
            connection = connections[DEFAULT_DB_ALIAS]
            es_connection = self.connection
            mappings_old = connection.ops.get_mappings(self.index_name, self.name)
            es_connection.indices.put_mapping(doc_type=self.name,
                                              mapping=self,
                                              indices=self.index_name)
            connection.ops.register_mapping_update(self.index_name, self, mappings_old)

        except Exception:
            # reindex
            # MergeMappingException
            # 1. create alt index
            # 2. export/import data to new index
            # 3. assign alias to new index
            # 4. delete old index
            # TODO: Implement model reindex
            import traceback
            logger.error(traceback.format_exc())
            logger.info(u'Could not update mappings for doc_type:"{}"'.format(self.name))

    def __repr__(self):
        return "<DocumentObjectField:%s>" % self.name

    def get_code(self, num=1):
        data = SortedDict(self.as_dict())
        data.pop("properties", [])
        var_name ="doc_%s"%self.name
        code= [var_name+" = "+self.__class__.__name__+"(name=%r, "%self.name+", ".join(["%s=%r"%(k,v) for k,v in list(data.items())])+")"]
        for name, field in list(self.properties.items()):
            num+=1
            vname, vcode = field.get_code(num)
            code.append(vcode)
            code.append("%s.add_property(%s)"%(var_name, vname))
