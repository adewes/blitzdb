import pytest
import pprint

from ..helpers.movie_data import Movie,Actor,Director

from .fixtures import backend
from blitzdb.backends.sql.relations import ManyToManyProxy
from blitzdb.backends.sql.queryset import QuerySet

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
    backend.save(space_odyssey)
    backend.save(brian_de_palma)
    backend.save(harrison_ford)

    backend.update(stanley_kubrick,{'favorite_actor' : al_pacino})
    backend.update(francis_coppola,{'favorite_actor' : robert_de_niro})
    backend.update(stanley_kubrick,{'best_movie' : space_odyssey})

    backend.save(the_godfather)
    backend.save(clockwork_orange)
    backend.save(scarface)

    backend.commit()

    director = backend.get(Director,{'name' : 'Stanley Kubrick'})

    assert isinstance(director.movies,QuerySet)
    assert len(director.movies) == 2
    assert isinstance(director.best_movie,Movie)
    assert director.best_movie.lazy
    assert isinstance(director.best_movie.best_of_director,Director)
    #this object is lazy
    assert director.best_movie.best_of_director.lazy
    #pk value is not defined since this object is populated through a relation
    assert not 'pk' in director.best_movie.best_of_director.lazy_attributes
    assert director.best_movie.best_of_director
    assert director.best_movie.best_of_director.pk
    #when asking for the pk we had to load the object from the DB using the relational data
    assert not director.best_movie.best_of_director.lazy
    assert director.best_movie.best_of_director == director

    #we test with a movie without a best_of_director relation...
    the_godfather.revert(backend)
    #this should raise an exception
    with pytest.raises(Director.DoesNotExist):
        assert the_godfather.best_of_director.eager
