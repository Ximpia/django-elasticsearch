# python
import logging
import pprint
from optparse import make_option
import sys

# django
from django.db import connections, DEFAULT_DB_ALIAS
from django.core.management.base import BaseCommand

__author__ = 'jorgealegre'

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    args = ''
    help = 'Get mappings for index and doc type'
    can_import_settings = True

    option_list = BaseCommand.option_list + (
        make_option('--index',
                    action='store',
                    dest='index',
                    default='',
                    help='Index name'),
        make_option('--doc_type',
                    action='store',
                    dest='doc_type',
                    default='',
                    help='Doc type'),
    )

    def handle(self, *args, **options):
        connection = connections[DEFAULT_DB_ALIAS]
        index_name = options.get('index', '')
        doc_type = options.get('doc_type', '')
        if index_name == '':
            self.stderr.write(u'index must be informed.')
            sys.exit(1)
        mappings = connection.ops.get_mappings(index_name, doc_type)
        self.stdout.write(pprint.PrettyPrinter(indent=4).pformat(mappings))
