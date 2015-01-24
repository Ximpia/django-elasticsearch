from pyes import mappings
from abc import ABCMeta, abstractmethod
import sys
from . import DjangoElasticEngineException
from django.utils.translation import ugettext_lazy as _

__author__ = 'jorgealegre'


def model_to_mapping(model, **kwargs):
    """
    This receives a model and generates the mapping

    :return:
    """
    meta = model._meta
    mapping = mappings.ObjectField()
    for field in meta.fields + meta.many_to_many:
        field_type = type(field).__name__
        if hasattr(getattr(sys.modules['__main__'], '{}Mapping'.format(field_type))):
            # django model field type
            field_mapping = getattr(sys.modules['__main__'], '{}Mapping'.format(field_type)).get(field, **kwargs)
            if field_mapping:
                mapping.add_property(field_mapping)
        elif hasattr(mappings, field_type):
            # ElasticSearch fields from pyes
            mapping.add_property(field)
        else:
            raise DjangoElasticEngineException(_(u'Field type {} not supported'.format(field_type)))
    return mapping


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
    def get(cls, field, **kwargs):
        """
        Generate mapping for IntegerField

        :param Field field: Django model field
        :return: Elastic mapping for field
        :rtype mappings.IntegerField
        """
        return mappings.IntegerField(name=field.name,
                                     **kwargs)


class PositiveSmallIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        pass


class SmallIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        pass


class PositiveIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        pass


class PositionFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        pass


class FloatFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field):
        pass


class DecimalFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        pass


class BooleanFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        pass


class NullBooleanFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        pass


class CharFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        return mappings.StringField(name=field.name,
                                    **kwargs)


class TextFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, analyzer='snowball', **kwargs):
        """

        :param field:
        :return:
        """
        return mappings.StringField(name=field.name,
                                    analyzer=analyzer,
                                    **kwargs)


class DateTimeFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        return mappings.DateField(name=field.name,
                                  format='%Y-%m-%dT%H:%M:%S',
                                  **kwargs)


class DateFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        return mappings.DateField(name=field.name,
                                  format='%Y-%m-%d',
                                  **kwargs)


class DictFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        pass


class SetFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        pass


class ListFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        pass
