import pytest
import pprint
import datetime

from blitzdb.backends.sql import Backend
from blitzdb.fields import DateTimeField
from blitzdb import Document
from sqlalchemy import create_engine
from sqlalchemy.types import String

class Actor(Document):

    created_at = DateTimeField(auto_now_add = True,indexed = True)

@pytest.fixture(scope="function")
def backend():

    engine = create_engine('sqlite:///:memory:', echo=True)
    return Backend(engine = engine)

def test_basics(backend):

    backend.create_schema()

    actor = Actor({'created_at' : datetime.datetime.now()})

    backend.save(actor)
    backend.commit()

    recovered_actor = backend.get(Actor,{})
#    assert isinstance(recovered_actor.created_at,datetime.datetime)
    assert recovered_actor.created_at == actor.created_at