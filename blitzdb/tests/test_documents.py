import pytest
import copy

from blitzdb import Document

@pytest.fixture(scope = "function")
def mockup_backend():

    class Backend(object):  

        def __init__(self):
            self.attributes = {'foo' : 'bar', 'baz' : 123}

        def get(self,DocumentClass,pk):
            return DocumentClass(copy.deepcopy(self.attributes))

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

    #Fetchin of attribute by class attribute

    doc = get_lazy_doc()

    assert doc._lazy == True
    assert doc.foo == 'bar'
    assert doc._lazy == False

    #Fetching of attribute by dict

    doc = get_lazy_doc()

    assert doc._lazy == True
    assert doc['foo'] == 'bar'
    assert doc._lazy == False

    #Getting all attributes

    doc = get_lazy_doc()

    assert doc._lazy == True
    attributes = doc.attributes
    del attributes['pk']
    assert attributes == mockup_backend.attributes
    assert doc._lazy == False

    #Deletion by dict

    doc = get_lazy_doc()

    assert doc._lazy == True
    del doc['foo']
    with pytest.raises(KeyError):
        doc['foo']
    assert doc._lazy == False

    #Deletion by attribute

    doc = get_lazy_doc()

    assert doc._lazy == True
    del doc.foo
    with pytest.raises(AttributeError):
        doc.foo
    assert doc._lazy == False

    #Update by dict

    doc = get_lazy_doc()

    assert doc._lazy == True
    doc['foo'] = 'faz'
    assert doc._lazy == False
    assert doc['foo'] == 'faz'

    #Update by attribute

    doc = get_lazy_doc()

    assert doc._lazy == True
    doc.foo = 'faz'
    assert doc._lazy == False
    assert doc.foo == 'faz'
    


def test_container_operations():

    attributes = {'foo' : 'bar','baz' : 1243, 'd' : {1 :3,4 :5},'l' : [1,2,3,4]}

    doc = Document(attributes)

    with pytest.raises(KeyError):
        doc['fooz']

    assert ('foo' in doc) == True
    assert ('fooz' in doc) == False
    assert list(doc.keys()) == list(attributes.keys())
    assert list(doc.values()) == list(attributes.values())
    assert doc.items() == attributes.items()

def test_different_primary_key_names():

    class MyDocument(Document):

        class Meta:
            primary_key = 'foobar'

    doc = MyDocument({'foo' : 'bar','foobar' : 1})

    assert doc.pk == 1
    doc.pk = 2
    assert doc.attributes['foobar'] == 2