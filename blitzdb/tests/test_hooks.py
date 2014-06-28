from __future__ import absolute_import

import pytest

from .fixtures import *

from blitzdb import Document
from blitzdb.tests.helpers.movie_data import Actor,Director,Movie

class MyDocument(Document):

    """
    We define a document class with pre-save and pre-delete hooks.
    """

    def pre_save(self):
        self.foo = "bar"

    def pre_delete(self):
        self.foo = "bar"

def test_pre_save_hook(backend,small_test_data):

    my_document = MyDocument({'test' : 123})

    backend.save(my_document)

    assert hasattr(my_document,'foo')
    assert my_document.foo == "bar"

def test_pre_delete_hook(backend,small_test_data):

    my_document = MyDocument({'test' : 123})
    my_document.pk = 1

    backend.delete(my_document)

    assert hasattr(my_document,'foo')
    assert my_document.foo == "bar"
