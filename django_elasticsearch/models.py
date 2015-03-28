# python
import logging

# django
from django.db import models
from django.utils.translation import ugettext_lazy as _
from django.contrib.auth.models import User

__author__ = 'jorgealegre'


DATE_CHUNKS_PER_DAY = 'per_day'
DATE_CHUNKS_PER_MONTH = 'per_month'
DATE_CHUNKS_CHOICE = (
    (DATE_CHUNKS_PER_DAY, _(u'Per Day')),
    (DATE_CHUNKS_PER_MONTH, _(u'Per Month')),
)

logger = logging.getLogger(__name__)


def get_settings_by_meta(meta_index):
    return {
        'number_of_replicas': meta_index['number_of_replicas'],
        'number_of_shards': meta_index['number_of_shards'],
    }


class BaseModel(models.Model):

    """

    ES index name:
    $appname-$modelname-$modelindex-$datecreated
    alias:
    $appname-$modelname-$modelindex

    Some cases we would want model forced into a model index, disallow from db default index

    created_by:
    {
        'id': id,
        'value': username,
    }
    """

    created_on = models.DateTimeField(auto_now_add=True)
    updated_on = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(User, null=True, blank=True)
    updated_by = models.ForeignKey(User, null=True, blank=True)

    class Meta:
        abstract = True
        indices = [
            {
                'by_user': {
                    'routing_field': 'user.id',
                    'number_of_replicas': 1,
                    'number_of_shards': 5,
                },
            }
        ]
