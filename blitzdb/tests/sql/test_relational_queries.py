 # -*- coding: utf-8 -*-
import pytest

from ..helpers.movie_data import Movie,Actor,Director

from .fixtures import backend
from blitzdb.backends.sql.relations import ManyToManyProxy

def prepare_data(backend):

    backend.init_schema()
    backend.create_schema()

    francis_coppola = Director({'name' : 'Francis Coppola'})
    stanley_kubrick = Director({'name' : 'Stanley Kubrick'})
    robert_de_niro = Actor({'name' : 'Robert de Niro','movies' : []})
    harrison_ford = Actor({'name' : 'Harrison Ford'})
    andreas_dewes = Actor({'name' : 'Andreas Dewes'})
    brian_de_palma = Director({'name' : 'Brian de Palma'})

    al_pacino = Actor({'name' : 'Al Pacino','movies' : [],'salary' : {'amount' : 100000000,'currency' : u'€'}})

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

    apocalypse_now = Movie({'title' : 'Apocalypse Now'})
    star_wars_v = Movie({'title' : 'Star Wars V: The Empire Strikes Back'})
    harrison_ford.movies = [star_wars_v]

    backend.save(robert_de_niro)
    backend.save(al_pacino)
    backend.save(francis_coppola)
    backend.save(andreas_dewes)
    backend.save(stanley_kubrick)
    backend.save(brian_de_palma)
    backend.save(harrison_ford)


    backend.update(scarface,{'best_actor' : al_pacino})
    backend.update(the_godfather,{'best_actor' : robert_de_niro})
    backend.update(stanley_kubrick,{'favorite_actor' : al_pacino})
    backend.update(francis_coppola,{'favorite_actor' : robert_de_niro})

    backend.save(the_godfather)
    backend.save(clockwork_orange)
    backend.save(space_odyssey)
    backend.save(scarface)

    backend.commit()

def test_queryset(backend):

    prepare_data(backend)


    al_pacino = backend.get(Actor,{'name' : 'Al Pacino'})
    robert_de_niro = backend.get(Actor,{'name' : 'Robert de Niro'})

    assert al_pacino.movies._queryset is None

    actors = backend.filter(Actor,{'movies' : {'$in' : al_pacino.movies} })

    assert actors
    assert len(actors) == 2
    assert robert_de_niro in actors
    #the queryset should not have been loaded from the DB
    assert al_pacino.movies._queryset
    assert al_pacino.movies._queryset.objects is None

def test_queryset_all(backend):

    prepare_data(backend)

    al_pacino = backend.get(Actor,{'name' : 'Al Pacino'})
    robert_de_niro = backend.get(Actor,{'name' : 'Robert de Niro'})

    assert al_pacino.movies._queryset is None

    actors = backend.filter(Actor,{'movies' : {'$all' : al_pacino.movies} })

    assert actors
    assert len(actors) == 1
    assert robert_de_niro not in actors
    #the queryset should not have been loaded from the DB
    assert al_pacino.movies._queryset
    assert al_pacino.movies._queryset.objects is None

def test_one_to_many(backend):

    prepare_data(backend)

    al_pacino = backend.get(Actor,{'name' : 'Al Pacino'})
    robert_de_niro = backend.get(Actor,{'name' : 'Robert de Niro'})

    assert al_pacino.movies._queryset is None

    movies = backend.filter(Movie,{'best_actor' : {'$in' : [al_pacino,robert_de_niro]}})

    assert movies