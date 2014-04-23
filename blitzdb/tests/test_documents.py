import pytest
import tempfile
import subprocess
import random
import time
import pymongo

from blitzdb import Document

@pytest.fixture(scope = "function")
def mockup_backend():

    class Backend(object):  

        def __init__(self):
            self.attributes = {'foo' : 'bar', 'baz' : 123}

        def get(self,DocumentClass,pk):
            return DocumentClass(self.attributes)

    return Backend()

def test_basic_attributes():

    attributes = {'foo' : 'bar','baz' : 1243, 'd' : {1 :3,4 :5},'l' : [1,2,3,4]}

    doc = Document(attributes)

    assert doc.foo == 'bar'
    assert doc.baz == 1243
    assert doc.d == {1 :3,4:5}
    assert doc.l == [1,2,3,4]

    assert doc.foo == doc['foo']
    assert doc.baz == doc['baz']
    assert doc.d == doc['d']

    assert doc.attributes == attributes

def test_attribute_deletion():

    attributes = {'foo' : 'bar','baz' : 1243, 'd' : {1 :3,4 :5},'l' : [1,2,3,4]}

    doc = Document(attributes)

    del doc.foo

    with pytest.raises(AttributeError):
        doc.foo

    with pytest.raises(KeyError):
        doc['foo']

    with pytest.raises(KeyError):
        del doc['foo']

    with pytest.raises(AttributeError):
        del doc.foo

def test_lazy_attributes(mockup_backend):

    def get_lazy_doc():
        return Document({'pk' : 1},lazy = True,default_backend = mockup_backend)

    doc = get_lazy_doc()

    assert doc._lazy == True
    assert doc.foo == 'bar'
    assert doc._lazy == False

    doc = get_lazy_doc()

    assert doc._lazy == True
    assert doc['foo'] == 'bar'
    assert doc._lazy == False

    doc = get_lazy_doc()

    assert doc._lazy == True
    assert doc.attributes == mockup_backend.attributes
    assert doc._lazy == False