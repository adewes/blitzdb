import pytest

from ..fixtures import test_mongo
from blitzdb import Document

if test_mongo:

    from ..fixtures import mongodb_backend

    def test_complex(mongodb_backend):

        c = 1j+4

        attributes = {'foo': c,}

        doc = Document(attributes)

        mongodb_backend.save(doc)
        mongodb_backend.commit()

        assert mongodb_backend.get(Document,{'pk' : doc.pk}) == doc
        assert mongodb_backend.get(Document,{'foo' : c}) == doc
        assert len(mongodb_backend.filter(Document,{'foo' : c})) == 1

        with pytest.raises(Document.DoesNotExist):
            mongodb_backend.get(Document,{'foo' : 1j+5})

        assert mongodb_backend.get(Document,{'foo' : c}).foo == c
