from __future__ import absolute_import


from .fixtures import *

from blitzdb import Document

import datetime

class MyDocument(Document):

    """
    We define a document class with pre-save and pre-delete hooks.
    """

    def before_save(self):
        self.foo = "bar"

    def before_delete(self):
        self.foo = "bar"

    def after_load(self):
        self.bar = "baz"

    def before_update(self,set_fields,unset_fields):
        set_fields['updated_at'] = datetime.datetime.now()

class MyDerivedDocument(MyDocument):
    pass


def test_after_load_hook(backend, small_test_data):

    my_document = MyDocument({'test': 123})
    backend.save(my_document)
    backend.commit()

    recovered_document = backend.get(MyDocument,{'pk' : my_document.pk})

    assert hasattr(recovered_document, 'bar')
    assert recovered_document.bar == "baz"

def test_before_save_hook(backend, small_test_data):

    my_document = MyDocument({'test': 123})
    backend.save(my_document)

    assert hasattr(my_document, 'foo')
    assert my_document.foo == "bar"

def test_before_update_hook(backend, small_test_data):

    my_document = MyDerivedDocument({'test': 123})
    backend.save(my_document)

    assert hasattr(my_document, 'foo')
    assert my_document.foo == "bar"

    backend.update(my_document,{'foo' : 'baz'})
    backend.commit()

    assert my_document.foo == 'baz'
    assert 'updated_at' in my_document
    assert isinstance(my_document.updated_at,datetime.datetime)


def test_before_delete_hook(backend, small_test_data):

    my_document = MyDocument({'test': 123})
    my_document.pk = 1

    backend.delete(my_document)

    assert hasattr(my_document, 'foo')
    assert my_document.foo == "bar"
