import pytest

from ..fixtures import test_mongo
from blitzdb import Document

if test_mongo:
    from ..fixtures import mongodb_backend

    def test_non_existing_key(mongodb_backend):

        attributes = {'foo': 'bar', 'baz': 1243, 'd': {1: 3, 4: 5}, 'l': [1, 2, 3, 4]}

        doc = Document(attributes)

        mongodb_backend.save(doc)
        mongodb_backend.commit()

        mongodb_backend.update(doc,['non_existing_key'])
        mongodb_backend.commit()

        assert mongodb_backend.get(Document,{'pk' : doc.pk}) == doc

    def test_basics(mongodb_backend):
        attributes = {'foo': 'bar', 'baz': 1243, 'd': {1: 3, 4: 5}, 'l': [1, 2, 3, 4]}

        doc = Document(attributes)
        mongodb_backend.save(doc)
        mongodb_backend.commit()

        doc.foobar = 'baz'
        mongodb_backend.update(doc,['foobar'])
        mongodb_backend.commit()

        assert mongodb_backend.get(Document,{'foobar' : 'baz'}) == doc

    def test_update_non_existing_document(mongodb_backend):
        attributes = {'foo': 'bar', 'baz': 1243, 'd': {1: 3, 4: 5}, 'l': [1, 2, 3, 4]}

        doc = Document(attributes)

        doc.foobar = 'baz'
        with pytest.raises(Document.DoesNotExist):
            mongodb_backend.update(doc,['foobar'])
            mongodb_backend.commit()

        with pytest.raises(Document.DoesNotExist):
            assert mongodb_backend.get(Document,{'foobar' : 'baz'})

    def test_deep_update(mongodb_backend):
        attributes = {'foo': {'bar' : 'baz'}, 'baz': 1243, 'd': {1: 3, 4: 5}, 'l': [1, 2, 3, 4]}

        doc = Document(attributes)
        mongodb_backend.save(doc)
        mongodb_backend.commit()

        mongodb_backend.update(doc,{'foo.bar' : 'bam'})
        mongodb_backend.commit()

        assert mongodb_backend.get(Document,{'foo.bar' : 'bam'}) == doc


        doc.foo['bar'] = 'squirrel'
        #we update using a list rather than a dict
        mongodb_backend.update(doc,['foo.bar'])
        mongodb_backend.commit()


        assert mongodb_backend.get(Document,{'foo.bar' : 'squirrel'}) == doc