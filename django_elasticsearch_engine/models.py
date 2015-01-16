from django.db.models import signals
from .manager import add_es_manager

__author__ = 'jorgealegre'

signals.class_prepared.connect(add_es_manager())
