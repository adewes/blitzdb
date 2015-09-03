import pytest
import pprint

from ..helpers.movie_data import Movie,Actor,Director

from .fixtures import backend
from blitzdb.backends.sql.relations import ManyToManyProxy

def test_basics(backend):

    backend.init_schema()
    backend.create_schema()

    francis_coppola = Director({'name' : 'Francis Coppola'})
    stanley_kubrick = Director({'name' : 'Stanley Kubrick'})
    robert_de_niro = Actor({'name' : 'Robert de Niro','movies' : []})
    harrison_ford = Actor({'name' : 'Harrison Ford'})
    brian_de_palma = Director({'name' : 'Brian de Palma'})

    al_pacino = Actor({'name' : 'Al Pacino','movies' : []})

    scarface = Movie({'title' : 'Scarface','director' : brian_de_palma})

    the_godfather = Movie({'title' : 'The Godfather',
                           'director' : francis_coppola})

    space_odyssey = Movie({'title' : '2001 - A space odyssey',
                           'director' : stanley_kubrick})

    clockwork_orange = Movie({'title' : 'A Clockwork Orange',
                              'director' : stanley_kubrick})

    robert_de_niro.movies.append(the_godfather)
    al_pacino.movies.append(the_godfather)
    al_pacino.movies.append(scarface)
    stanley_kubrick.favorite_actor = al_pacino
    francis_coppola.favorite_actor = robert_de_niro

    apocalypse_now = Movie({'title' : 'Apocalypse Now'})
    star_wars_v = Movie({'title' : 'Star Wars V: The Empire Strikes Back'})
    harrison_ford.movies = [star_wars_v]

    backend.save(the_godfather)
    backend.save(robert_de_niro)
    backend.save(al_pacino)
    backend.save(francis_coppola)
    backend.save(stanley_kubrick)
    backend.save(clockwork_orange)
    backend.save(space_odyssey)
    backend.save(brian_de_palma)
    backend.save(scarface)
    backend.save(harrison_ford)
    backend.commit()

    actors = backend.filter(Actor,{},include = (('movies',('director',),'title'),('movies','year')))

    assert isinstance(actors[0].movies,ManyToManyProxy)
    assert actors.objects is not None
    assert not actors[0].lazy
    assert actors[0].movies._queryset is not None
    assert actors[0].movies._queryset.objects is not None
    assert actors[0].movies[0].lazy