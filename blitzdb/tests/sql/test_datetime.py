import pytest
import pprint
import datetime

from blitzdb.backends.sql import Backend
from blitzdb.fields import DateTimeField
from blitzdb import Document
from sqlalchemy import create_engine
from sqlalchemy.types import String

class Actor(Document):

    class Meta(Document.Meta):
        autoregister = False

    created_at = DateTimeField(auto_now_add = True,indexed = True)

@pytest.fixture(scope="function")
def backend():

    engine = create_engine('sqlite:///:memory:', echo=True)
    backend = Backend(engine = engine,autodiscover_classes = False)
    backend.register(Actor)
    backend.init_schema()
    backend.create_schema()
    return backend

def test_basics(backend):

    actor = Actor({'created_at' : datetime.datetime.now()})

    backend.save(actor)
    backend.commit()

    recovered_actor = backend.get(Actor,{})
#    assert isinstance(recovered_actor.created_at,datetime.datetime)
    assert recovered_actor.created_at == actor.created_at