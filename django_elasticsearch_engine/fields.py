# python
import logging
import pprint

# django

# pyes
from pyes import mappings

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

    def save(self):
        if self.connection is None:
            raise RuntimeError(u"No connection available")
        result = self.connection.indices.put_mapping(doc_type=self.name,
                                                     mapping=self.as_dict(),
                                                     indices=self.index_name)
        logger.info(u'result: {}'.format(pprint.PrettyPrinter(indent=4).pformat(result)))


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
        result = self.connection.indices.put_mapping(doc_type=self.name,
                                                     mapping=self.as_dict(),
                                                     indices=self.index_name)
        logger.info(u'result: {}'.format(pprint.PrettyPrinter(indent=4).pformat(result)))
