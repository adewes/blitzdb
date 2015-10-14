from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor, Director, Movie


def test_basic_delete(backend, small_test_data):

    with backend.transaction():
        backend.filter(Actor, {}).delete()
    
    assert len(backend.filter(Actor, {})) == 0


def test_basic_storage(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    assert len(backend.filter(Movie, {})) == len(movies)
    assert len(backend.filter(Actor, {})) == len(actors)


def test_delete(backend):

    actor = Actor({'foo' : 'bar'})

    with backend.transaction():
        backend.save(actor)

    assert actor.foo == 'bar'

    assert backend.get(Actor,{'pk' : actor.pk}).foo == 'bar'

    del actor.foo

    with pytest.raises(AttributeError):
        actor.foo

    with pytest.raises(KeyError):
        actor['foo']

    with backend.transaction():
        backend.save(actor)

    with pytest.raises(AttributeError):
        backend.get(Actor,{'pk' : actor.pk}).foo


def test_negative_indexing(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    actors = backend.filter(Actor, {})

    assert actors[-1] == actors[len(actors) - 1]
    assert actors[-10:-1] == actors[len(actors) - 10:len(actors) - 1]
    assert actors[-len(actors):-1] == actors[0:len(actors) - 1]

    # To do: Make step tests for file backend (MongoDB does not support this)
#    assert actors[-10:-1:2] == actors[len(actors)-10:len(actors)-1:2]


def test_missing_keys_in_slice(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    actors = backend.filter(Actor, {})

    assert actors[:] == actors
    assert actors[1:] == actors[1:len(actors)]
    assert actors[:len(actors)] == actors[0:len(actors)]


def test_query_set(backend):

    actors = [Actor({'name': 'bar', 'is_funny': True}),
              Actor({'name': 'baz', 'is_funny': False}),
              Actor({'name': 'baz', 'is_funny': False}),
              Actor({'name': 'bar', 'is_funny': False})
              ]

    with backend.transaction():
        for actor in actors:
            backend.save(actor)

    queryset = backend.filter(Actor, {'name': 'bar','is_funny' : True})

    assert queryset.next() == actors[0]

def test_and_queries(backend):

    with backend.transaction():
        backend.save(Actor({'name': 'bar', 'is_funny': False}))
        backend.save(Actor({'name': 'baz', 'is_funny': False}))
        backend.save(Actor({'name': 'baz', 'is_funny': True}))
        backend.save(Actor({'name': 'bar', 'is_funny': True}))

    assert len(backend.filter(Actor, {'name': 'bar'})) == 2
    assert len(backend.filter(Actor, {'is_funny': False})) == 2
    assert len(backend.filter(Actor, {'name': 'bar', 'is_funny': False})) == 1
    assert len(backend.filter(Actor, {'name': 'baz', 'is_funny': False})) == 1
    assert len(backend.filter(Actor, {'name': 'bar', 'is_funny': False})) == 1
    assert len(backend.filter(Actor, {'name': 'baz', 'is_funny': False})) == 1


def test_list_query_multiple_items(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    actor = None
    i = 0
    while not actor or len(actor.movies) < 2:
        actor = actors[i]
        i += 1

    assert len(backend.filter(Actor, {'movies': {'$all' : actor.movies}}))
    assert actor in backend.filter(Actor, {'movies': {'$all' : actor.movies}})


def test_invalid_query(backend, small_test_data):

    with pytest.raises(BaseException):
        backend.filter(Actor, {'$in' : movie.cast})


def test_non_indexed_delete(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    with backend.transaction():
        for movie in movies:
            if movie.get('director'):
                director = movie.director
                for directed_movie in backend.filter(Movie,{'director' : director}):
                    backend.update(directed_movie,unset_fields = ['director'])
                backend.delete(director)

    directors = backend.filter(Director,{})

    for director in directors:
        assert len(backend.filter(Movie,{'director' : director})) == 0

def test_default_backend(backend, small_test_data):

    movies = backend.filter(Movie, {})
    old_len = len(movies)
    movie = movies[0]
    with backend.transaction():
        movie.delete()

    with pytest.raises(Movie.DoesNotExist):
        backend.get(Movie, {'pk': movie.pk})

    assert old_len == len(backend.filter(Movie, {})) + 1


def test_index_reloading(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    with backend.transaction():
        backend.filter(Actor, {'movies': movies[0]}).delete()
    
    assert list(backend.filter(Actor, {'movies': movies[0]})) == []
