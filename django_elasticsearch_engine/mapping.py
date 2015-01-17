from pyes import mappings
from abc import ABCMeta, abstractmethod
import sys

__author__ = 'jorgealegre'


def model_to_mapping(model):
    """
    This receives a model and generates the mapping

    :return:
    """
    meta = model._meta
    mapping = mappings.ObjectField()
    for field in meta.fields + meta.many_to_many:
        field_type = type(field).__name__
        field_mapping = getattr(sys.modules['__main__'], '{}Mapping'.format(field_type)).get(field)
        if field_mapping:
            mapping.add_property(field_mapping)
    return mapping


# define an abstract class with get method for FieldMapping


class FieldMapping(object):
    __metaclass__ = ABCMeta

    @classmethod
    @abstractmethod
    def get(cls, field):
        """
        Generate mapping for Field

        :param Field field: Django model field
        :return: Elastic mapping for field
        :rtype mappings.IntegerField
        """
        pass


class IntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        """
        Generate mapping for IntegerField

        :param Field field: Django model field
        :return: Elastic mapping for field
        :rtype mappings.IntegerField
        """
        return mappings.IntegerField(name=field.name,
                                     store=True)
FieldMapping.register(IntegerFieldMapping)


class PositiveSmallIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(PositiveSmallIntegerFieldMapping)


class SmallIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(SmallIntegerFieldMapping)


class PositiveIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(PositiveIntegerFieldMapping)


class PositionFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(PositionFieldMapping)


class FloatFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(FloatFieldMapping)


class DecimalFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(DecimalFieldMapping)


class BooleanFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(BooleanFieldMapping)


class NullBooleanFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass
FieldMapping.register(NullBooleanFieldMapping)
