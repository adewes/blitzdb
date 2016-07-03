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

        with pytest.raises(ValueError):
            assert mongodb_backend.get(Document,{'foo' : c})

        assert mongodb_backend.get(Document,{'foo.r' : c.real,'foo.i' : c.imag}).foo == c
