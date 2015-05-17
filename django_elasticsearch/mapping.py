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
import fields

__author__ = 'jorgealegre'


logger = logging.getLogger(__name__)


def model_to_mapping(model, connection, index_name, **kwargs):
    """
    This receives a model and generates the mapping

    :return:
    """
    meta = model._meta
    logger.debug(u'meta: {} fields: {}'.format(meta, meta.fields + meta.many_to_many))
    mapping = fields.DocumentObjectField(
        name=kwargs.get('name', model._meta.db_table),
        connection=connection,
        index_name=index_name,
    )
    if '_routing' in kwargs:
        mapping['_routing'] = kwargs['_routing']
    for field in meta.fields + meta.many_to_many:
        field_type = type(field).__name__
        if hasattr(sys.modules[ENGINE + '.mapping'], '{}Mapping'.format(field_type)):
            # django model field type
            field_mapping = getattr(sys.modules[ENGINE + '.mapping'], '{}Mapping'
                                    .format(field_type)).get(field)
            if field_mapping:
                mapping.add_property(field_mapping)
        elif hasattr(mappings, field_type):
            # ElasticSearch fields from pyes
            mapping.add_property(field)
        else:
            raise DjangoElasticEngineException(_(u'Field type {} not supported'.format(field_type)))
    logger.info(u'model_to_mapping :: model: {} index_name: {}'.format(
        model._meta.db_table,
        index_name
    ))
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
        return fields.StringField(name=field.name,
                                  store=True,
                                  index='not_analyzed')


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
        """
        Generate mapping for PositiveSmallInteger

        :param field:
        :param kwargs:
        :return:
        """
        return mappings.IntegerField(name=field.name,
                                     **kwargs)


class SmallIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        return mappings.IntegerField(name=field.name,
                                     **kwargs)


class PositiveIntegerFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        return mappings.IntegerField(name=field.name,
                                     **kwargs)


class PositionFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        return mappings.GeoPointField(name=field.name,
                                      **kwargs)


class FloatFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        return mappings.FloatField(name=field.name,
                                   **kwargs)


class DecimalFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        return mappings.DoubleField(name=field.name,
                                    **kwargs)


class BooleanFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        return mappings.BooleanField(name=field.name,
                                     **kwargs)


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
                                   fields={field.name: fields.StringField(name=field.name,
                                                                          index="analyzed",
                                                                          term_vector="with_positions_offsets"),
                                           "raw": fields.StringField(name="raw",
                                                                     index="not_analyzed")}
                                   )


class TextFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, analyzer='snowball', **kwargs):
        """

        :param field:
        :return:
        """
        return fields.StringField(name=field.name,
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
        return mappings.ObjectField(name=field.name,
                                    **kwargs)


class SetFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        return mappings.ObjectField(name=field.name,
                                    **kwargs)


class ListFieldMapping(FieldMapping):

    @classmethod
    def get(cls, field, **kwargs):
        """

        :param field:
        :return:
        """
        return mappings.ObjectField(name=field.name,
                                    **kwargs)
