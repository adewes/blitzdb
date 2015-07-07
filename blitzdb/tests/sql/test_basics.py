import pytest
import pprint

from blitzdb.backends.sql import Backend
from blitzdb import Document
from sqlalchemy import create_engine
from sqlalchemy.types import String

@pytest.fixture(scope="function")
def backend():

    engine = create_engine('sqlite:///:memory:', echo=True)
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
            },
            {
                'sql' : {
                    'field' : 'title',
                    'type' : String,
                }
            }
        ]

        relations = [
            {
                'field' : 'director',
                'type' : 'ForeignKey',
                'related' : 'Actor',
                'sparse' : True,
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
#                'qualifier' : 'role'
            },
        ]

class Director(Document):
    
    class Meta(Document.Meta):

        indexes = [
            {
                'sql' : {
                    'field' : 'name',
                    'type' : String,
                }
            }
        ]

def test_basics(backend):

    backend.create_schema()

    francis_coppola = Director({'name' : 'Francis Coppola'})
    backend.save(francis_coppola)

    the_godfather = Movie({'title' : 'The Godfather','director' : francis_coppola})
    apocalypse_now = Movie({'title' : 'Apocalypse Now'})

    backend.save(the_godfather)
    backend.save(apocalypse_now)

    marlon_brando = Actor({'name': 'Marlon Brando', 'movies' : [the_godfather,apocalypse_now]})
    al_pacino = Actor({'name': 'Al Pacino', 'movies' : [the_godfather]})

    backend.save(marlon_brando)
    backend.save(al_pacino)

    result = backend.filter(Actor,{'movies' : {'$all' : [the_godfather,apocalypse_now]}})

    assert len(result) == 1
    assert marlon_brando in result

    result = backend.filter(Actor,{'movies.title' : {'$all' : ['The Godfather','Apocalypse Now']}})

    assert len(result) == 1
    assert marlon_brando in result

    result = backend.filter(Actor,{'movies' : {'$elemMatch' : {'title' : 'The Godfather'}}})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies' : {'$all' : [{'$elemMatch' : {'title' : 'The Godfather'}},{'$elemMatch' : {'title' : 'Apocalypse Now'}}]}})

    assert len(result) == 1
    assert marlon_brando in result

    result = backend.filter(Actor,{'movies.title' : 'The Godfather'})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result


    result = backend.filter(Actor,{'movies' : {'$in' : [the_godfather,apocalypse_now]}})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies.title' : {'$in' : ['The Godfather','Apocalypse Now']}})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'$or' : [{'movies.title' : 'The Godfather'},{'movies.title' : 'Apocalypse Now'}]})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result


    result = backend.filter(Movie,{'director' : francis_coppola})

    assert len(result) == 1
    assert the_godfather in result