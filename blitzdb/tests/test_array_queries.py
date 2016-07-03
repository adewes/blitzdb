import pytest
from .helpers.movie_data import Movie,Actor,Director
from .fixtures import *
from blitzdb.backends.file import Backend as FileBackend
from blitzdb.backends.mongo import Backend as MongoBackend
    
def test_array_queries(backend):

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

    backend.commit()

    result = backend.filter(Actor,{'movies' : the_godfather})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies' : {'$all' : [the_godfather,apocalypse_now]}})

    assert len(result) == 1
    assert marlon_brando in result

    result = backend.filter(Actor,{'movies.title' : {'$all' : ['The Godfather','Apocalypse Now']}})

    assert len(result) == 1
    assert marlon_brando in result

    if not isinstance(backend,FileBackend):

        #$elemMatch queries are currently not supported by the file backend.

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