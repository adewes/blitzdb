import pytest

from ..fixtures import test_mongo
from blitzdb import Document

if test_mongo:
    from blitzdb.backends.mongo.backend import DotEncoder
    from ..fixtures import mongodb_backend

    def test_dots(mongodb_backend):

        attributes = {'foo.baz.bam': 'blub', 'foo' : {'baz' : {'bam' : 'bar'}} }

        doc = Document(attributes)

        mongodb_backend.save(doc)
        mongodb_backend.commit()

        assert mongodb_backend.get(Document,{'pk' : doc.pk}) == doc

        #Dotted queries work as one would expect
        assert mongodb_backend.get(Document,{'foo.baz.bam' : 'bar'}) == doc

        #Filter queries too
        assert len(mongodb_backend.filter(Document,{'foo.baz.bam' : 'bar'})) == 1

        #When using dots in queries, we will NOT match the dotted field
        with pytest.raises(Document.DoesNotExist):
            mongodb_backend.get(Document,{'foo.baz.bam' : 'blub'})

        #If we escape the dots, the query should work as we expect
        assert mongodb_backend.get(Document,DotEncoder.encode({'foo.baz.bam' : 'blub'},[])) == doc