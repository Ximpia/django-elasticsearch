# python

# django

# pyes
from pyes import mappings

__author__ = 'jorgealegre'


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
