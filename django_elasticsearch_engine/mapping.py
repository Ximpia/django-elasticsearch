from pyes import mappings

__author__ = 'jorgealegre'


def model_to_mapping(model):
    """
    This receives a model and generates the mapping

    :return:
    """
    mapping = mappings.ObjectField()
    # add fields from inspection to mapping
    return mapping


class IntegerField(object):

    def __init__(self):
        pass
