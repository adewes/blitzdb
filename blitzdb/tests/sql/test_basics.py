import pytest
import pprint

from ..helpers.movie_data import Movie,Actor,Director

from .fixtures import backend

def test_multiple_joins(backend):

    francis_coppola = Director({'name' : 'Francis Coppola'})
    backend.save(francis_coppola)

    al_pacino = Actor({'name': 'Al Pacino'})
    the_godfather = Movie({'title' : 'The Godfather','director' : francis_coppola})
    al_pacino.movies = [the_godfather]

    backend.save(the_godfather)
    backend.update(the_godfather,{'best_actor' : al_pacino})

    result = backend.filter(Movie,{'director.name' : francis_coppola.name,'best_actor.name' : 'Al Pacino'})
    assert len(result) == 1
    assert the_godfather in result

    result = backend.filter(Movie,{'director.name' : {'$in' : [francis_coppola.name,'Al Pacino']}})
    assert len(result) == 1
    assert the_godfather in result

    result = backend.filter(Movie,{'director.name' : {'$in' : []}})
    assert len(result) == 0


def test_basics(backend):

    francis_coppola = Director({'name' : 'Francis Coppola'})
    backend.save(francis_coppola)

    the_godfather = Movie({'title' : 'The Godfather','director' : francis_coppola})
    apocalypse_now = Movie({'title' : 'Apocalypse Now'})
    star_wars_v = Movie({'title' : 'Star Wars V: The Empire Strikes Back'})

    backend.save(the_godfather)
    backend.save(apocalypse_now)

    marlon_brando = Actor({'name': 'Marlon Brando', 'movies' : [the_godfather,apocalypse_now]})
    al_pacino = Actor({'name': 'Al Pacino', 'movies' : [the_godfather]})
    francis_coppola.favorite_actor = al_pacino

    backend.save(marlon_brando)
    backend.save(al_pacino)
    backend.save(francis_coppola)
    backend.commit()

    result = backend.filter(Movie,{'director.name' : francis_coppola.name})
    assert len(result) == 1
    assert the_godfather in result

    result = backend.filter(Movie,{'director.name' : {'$in' : [francis_coppola.name,'Clint Eastwood']}})
    assert len(result) == 1
    assert the_godfather in result

    result = backend.filter(Actor,{'movies' : {'$all' : [the_godfather,apocalypse_now]}})

    assert len(result) == 1
    assert marlon_brando in result

    result = backend.filter(Actor,{'movies' : {'$in' : [the_godfather,apocalypse_now]}})

    assert marlon_brando in result
    assert al_pacino in result
    assert len(result) == 2

    result = backend.filter(Actor,{'movies.title' : 'The Godfather'})

    assert len(result) == 2
    assert marlon_brando in result

    result = backend.filter(Actor,{'movies' : {'$elemMatch' : {'title' : 'The Godfather'}}})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies' : {'$all' : [{'$elemMatch' : {'title' : 'The Godfather'}},{'$elemMatch' : {'title' : 'Apocalypse Now'}}]}})

    assert len(result) == 1
    assert marlon_brando in result

    result = backend.filter(Actor,{'movies' : {'$all' : [the_godfather,apocalypse_now]}})

    assert len(result) == 1
    assert marlon_brando in result
    assert al_pacino not in result

    with pytest.raises(AttributeError):
        #this query is ambiguous and hence not supported
        result = backend.filter(Actor,{'movies' : [the_godfather,apocalypse_now]})

    result = backend.filter(Actor,{'movies.title' : 'The Godfather'})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result


    result = backend.filter(Actor,{'movies' : {'$in' : [the_godfather,apocalypse_now]}})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies.title' : 'The Godfather'})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies.director.name' : {'$in' : ['Francis Coppola']}})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies.director.favorite_actor.name' : {'$in' : ['Al Pacino']}})

    assert len(result) == 2
    assert marlon_brando in result
    assert al_pacino in result

    result = backend.filter(Actor,{'movies.title' : {'$nin' : ['The Godfather','Apocalypse Now']}})

    assert len(result) == 0

    result = backend.filter(Actor,{'$or' : [{'movies.title' : 'The Godfather'},{'movies.title' : 'Apocalypse Now'}]})

    assert marlon_brando in result
    assert al_pacino in result
    assert len(result) == 2


    result = backend.filter(Movie,{'director' : francis_coppola})

    assert len(result) == 1
    assert the_godfather in result