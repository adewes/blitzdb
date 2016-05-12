import pytest
import pprint

from ..helpers.movie_data import Movie,Actor,Director,Document
from blitzdb.backends.sql.relations import ManyToManyProxy

from .fixtures import backend

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

    apocalypse_now = Movie({'title' : 'Apocalypse Now'})
    star_wars_v = Movie({'title' : 'Star Wars V: The Empire Strikes Back'})
    harrison_ford.movies = [star_wars_v]

    backend.save(robert_de_niro)
    backend.save(al_pacino)
    backend.save(francis_coppola)
    backend.save(stanley_kubrick)
    backend.save(brian_de_palma)
    backend.save(harrison_ford)

    backend.update(stanley_kubrick,{'favorite_actor' : al_pacino})
    backend.update(francis_coppola,{'favorite_actor' : robert_de_niro})

    backend.save(the_godfather)
    backend.save(clockwork_orange)
    backend.save(space_odyssey)
    backend.save(scarface)

    backend.commit()


    actor = backend.get(Actor,{'name' : 'Al Pacino'})

    assert isinstance(actor.movies,ManyToManyProxy)

    assert the_godfather in actor.movies
    assert scarface in actor.movies
    assert len(actor.movies) == 2

    with backend.transaction():

        actor.movies.remove(scarface)

    assert scarface not in actor.movies
    assert len(actor.movies) == 1

    actor.movies.append(scarface)

    assert len(actor.movies) == 2
    assert scarface in actor.movies

    #this should have no effect

    actor.movies.append(scarface)

    assert len(actor.movies) == 2
    assert scarface in actor.movies

    actor.movies.extend([scarface,the_godfather])

    assert len(actor.movies) == 2
    assert scarface in actor.movies

    actor.movies.extend([scarface,the_godfather,star_wars_v])

    assert len(actor.movies) == 3
    assert star_wars_v in actor.movies

    assert len(actor.movies[1:]) == 2
    assert len(actor.movies[1:2]) == 1
    assert len(actor.movies[:-1]) == 2
    assert len(actor.movies[1:-1]) == 1

def test_self_reference(backend):


    from blitzdb.fields import ManyToManyField
    class MovieMovie(Document):

        related_movies = ManyToManyField('MovieMovie',backref = 'related_movies_backref')

    backend.init_schema()
    backend.register(MovieMovie)
    backend.create_schema()
