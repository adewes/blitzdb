from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor, Movie

def test_nested_value(backend):

    actor = Actor({'name' : 'Robert de Niro'})
    movie = Movie({'best_actor' : actor,'title' : 'The Godfather'})

    backend.save(actor)
    actor.movies = [movie]
    backend.save(actor)
    backend.commit()
    backend.save(movie)
    backend.commit()

    recovered_actor = backend.get(Actor,{'pk' : actor.pk})

    assert recovered_actor == actor
    assert movie in recovered_actor.movies
    assert recovered_actor.movies[0] == movie
    assert 'best_actor' in recovered_actor.movies[0]
    assert recovered_actor.movies[0].best_actor == recovered_actor

    recovered_actors = backend.filter(Actor,{'movies.title' : 'The Godfather'})
    assert len(recovered_actors) == 1
    assert actor in recovered_actors

