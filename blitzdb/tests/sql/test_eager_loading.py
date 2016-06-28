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

    prepare_data(backend)

    al_pacino = backend.get(Actor,{'name' : 'Al Pacino'},include = ('best_movies',))
    scarface = backend.get(Movie,{'title' : 'Scarface'})
    the_godfather = backend.get(Movie,{'title' : 'The Godfather'})

    assert len(backend.filter(Movie,{'best_actor' : al_pacino})) == 2

    assert al_pacino.best_movies.objects
    assert len(al_pacino.best_movies) == 2
    assert the_godfather in al_pacino.best_movies
    assert scarface in al_pacino.best_movies

def test_one_to_many_lazy(backend):

    prepare_data(backend)

    al_pacino = backend.get(Actor,{'name' : 'Al Pacino'})
    scarface = backend.get(Movie,{'title' : 'Scarface'})
    the_godfather = backend.get(Movie,{'title' : 'The Godfather'})

    assert len(backend.filter(Movie,{'best_actor' : al_pacino})) == 2

    assert al_pacino.best_movies.objects is None
    assert len(al_pacino.best_movies) == 2
    assert the_godfather in al_pacino.best_movies
    assert scarface in al_pacino.best_movies

def test_basics(backend):

    prepare_data(backend)

    actors = backend.filter(Actor,{},include = (('movies',('director',),'title'),('movies','year')))

    for actor in actors:
        assert isinstance(actor.movies,ManyToManyProxy)
        assert actors.objects is not None
        assert not actor.lazy
        assert actor.movies._objects is not None
        if actor.movies:
            assert actor.movies[0].lazy

    actor = backend.get(Actor,{'name' : 'Andreas Dewes'},include = (('movies',('director','favorite_actor')),) )

    assert isinstance(actor,Actor)
    assert not actor.lazy
    assert isinstance(actor.movies,ManyToManyProxy)
    assert actor.movies._objects is not None
    assert actor.movies._objects == [] #no movies yet :(

def test_raw(backend):

    prepare_data(backend)

    actors = backend.filter(Actor,{},include = (('movies',('director',),'title'),('movies','year'),'gross_income_m'),raw = True)

    assert isinstance(actors[0],dict)
    assert isinstance(actors[0]['movies'],list)

def test_include_with_only_raw(backend):

    prepare_data(backend)

    actors = backend.filter(Actor,{},include = (('movies',('director',),'title'),('movies','year')),only = ('gross_income_m',),raw = True)

    assert isinstance(actors[0],dict)
    assert set(actors[0].keys()) == set(('movies','gross_income_m','pk'))
    assert isinstance(actors[0]['movies'],list)

def test_include_with_only(backend):

    prepare_data(backend)

    actors = backend.filter(Actor,{},include = (('movies',('director',),'title','year'),('movies')),only = ('gross_income_m',))

    assert isinstance(actors[0],Actor)
    assert actors[0].lazy
    assert set(actors[0].lazy_attributes.keys()) == set(('related_role_actor','actor_movie_movies','actor_movie_cast','actor_food_favorite_food','favorite_food','best_movies','related_director_favorite_actor','movies','gross_income_m','pk','related_movie_cast'))
    assert isinstance(actors[0]['movies'],ManyToManyProxy)
    assert actors[0]['movies']._objects is not None


def test_only(backend):

    prepare_data(backend)

    actors = backend.filter(Actor,{},only = ('gross_income_m',))

    assert isinstance(actors[0],Actor)
    assert actors[0].lazy
    assert set(actors[0].lazy_attributes.keys()) == set(('related_role_actor','actor_movie_movies','actor_movie_cast','actor_food_favorite_food','favorite_food','related_director_favorite_actor','best_movies','movies','gross_income_m','pk','related_movie_cast'))
    assert isinstance(actors[0]['movies'],ManyToManyProxy)
    assert actors[0]['movies']._objects is None
