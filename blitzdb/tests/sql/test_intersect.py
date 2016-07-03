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

    backend.update(the_godfather,{'best_actor' : al_pacino})
    backend.update(scarface,{'best_actor' : al_pacino})

    backend.update(stanley_kubrick,{'favorite_actor' : al_pacino})
    backend.update(francis_coppola,{'favorite_actor' : robert_de_niro})

    backend.save(the_godfather)
    backend.save(clockwork_orange)
    backend.save(space_odyssey)
    backend.save(scarface)

    backend.commit()

def test_one_to_many_include(backend):

    if str(backend.engine.url).startswith('sqlite://'):
        import sqlite3
        version = [int(s) for s in sqlite3.sqlite_version.split('.')]
        if version[0] < 3 or (version[0] == 3 and version[1] < 8):
            print("No support for common table expression in your SQLite version, skipping this test...")
            return

    prepare_data(backend)

    al_pacino = backend.get(Actor,{'name' : 'Al Pacino'},include = ('best_movies',))
    scarface = backend.get(Movie,{'title' : 'Scarface'})
    the_godfather = backend.get(Movie,{'title' : 'The Godfather'})


    results = backend.filter(Movie,{'best_actor' : al_pacino})
    assert len(results) == 2

    assert len(results.filter({'title' : 'Scarface'})) == 1