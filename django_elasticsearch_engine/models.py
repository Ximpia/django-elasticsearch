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
    user = models.ForeignKey(User, null=True, blank=True)

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


class ExampleModel(BaseModel):

    @classmethod
    def main_boost(cls, obj):
        return obj.number_likes*2.5

    def __unicode__(self):
        return ''

    class Meta:
        indices = [
            {
                'by_other_field': {
                    'routing': 'user.id',   # routing when index and query
                    'number_of_replicas': 1,
                    'number_of_shards': 5,
                    'date_chunks': DATE_CHUNKS_PER_DAY,  # When have indexes by day or month
                    'boost_function': 'main_boost',   # Function to apply boost for document
                    'boost': {  # boost is collection of boost by score defined here
                        'field': 2.0,
                    },
                    'is_default': True,
                },
            }
        ]
        disable_default_index = True    # When save, we don't write to project default index, useful for log type index
