import pytest
import pprint

from blitzdb.backends.sql import Backend
from blitzdb import Document
from sqlalchemy import create_engine
from sqlalchemy.types import String

@pytest.fixture(scope="function")
def backend():

    engine = create_engine('sqlite:///:memory:', echo=False)
    return Backend(engine = engine)

class Movie(Document):
    pass

class Actor(Document):
    
    class Meta(Document.Meta):

        indexes = {
            'name' : {
                'sql_type' : String
            }
        }

        many_to_many_fields = {
            'movies' : {
                'to' : Movie,
            }
        }

def test_basics(backend):

    backend.create_schema()