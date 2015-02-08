# python
import logging

# pyes
from pyes import mappings
from abc import ABCMeta, abstractmethod
import sys
from . import DjangoElasticEngineException
from django.utils.translation import ugettext_lazy as _

# django

# djes
from . import ENGINE

__author__ = 'jorgealegre'


logger = logging.getLogger(__name__)


def model_to_mapping(model, **kwargs):
    """
    This receives a model and generates the mapping

    :return:
    """
    meta = model._meta
    logger.debug(u'meta: {} fields: {}'.format(meta, meta.fields + meta.many_to_many))
    # mapping = mappings.ObjectField()
    mapping = mappings.DocumentObjectField(
        connection=None,
        index_name='',
    )
    for field in meta.fields + meta.many_to_many:
        field_type = type(field).__name__
        if hasattr(sys.modules[ENGINE + '.mapping'], '{}Mapping'.format(field_type)):
            # django model field type
            field_mapping = getattr(sys.modules[ENGINE + '.mapping'], '{}Mapping'
                                    .format(field_type)).get(field, **kwargs)
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


class AutoFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """
        Mapping for AutoField
        :param field:
        :param kwargs:
        :return:
        """
        return mappings.StringField(name=field.name, store=True)


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
        Mapping for CharField

        :param field:
        :return:
        """
        return mappings.MultiField(name=field.name,
                                   fields={field.name:mappings.StringField(name=field.name,
                                                                           index="not_analyzed",
                                                                           store=True),
                                           "tk": mappings.StringField(name="tk", store=True,
                                                                      index="analyzed",
                                                                      term_vector="with_positions_offsets")}
                                   )


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
