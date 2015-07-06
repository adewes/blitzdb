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
    
    class Meta(Document.Meta):

        indexes = [
            {
                'sql' : {
                    'field' : 'tags',
                    'list' : True,
                    'type' : String,
                }
            }
        ]

        relations = [
            {
                'field' : 'director',
                'type' : 'ForeignKey',
                'related' : 'Actor',
            },
        ]

class Actor(Document):
    
    class Meta(Document.Meta):

        indexes = [
            {
                'sql' : {
                    'field' : 'name',
                    'type' : String,
                }
            }
        ]

        """
        Relations to other tables
        """

        relations = [
            {
                'field' : 'movies',
                'type' : 'ManyToMany',
                'related' : 'Movie',
                'qualifier' : 'role'
            },
            {
                'field' : 'director',
                'type' : 'ForeignKey',
                'related' : 'Movie',
                'qualifier' : 'role'
            },
        ]

def test_basics(backend):

    backend.create_schema()