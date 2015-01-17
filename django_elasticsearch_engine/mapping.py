from pyes import mappings
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


class IntegerFieldMapping(object):

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
