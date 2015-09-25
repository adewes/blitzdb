from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor, Director, Movie


def test_basic_delete(backend, small_test_data):

    backend.filter(Actor, {}).delete()
    backend.commit()

    assert len(backend.filter(Actor, {})) == 0


def test_basic_storage(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    assert len(backend.filter(Movie, {})) == len(movies)
    assert len(backend.filter(Actor, {})) == len(actors)


def test_delete(backend):

    actor = Actor({'foo' : 'bar'})

    backend.save(actor)
    backend.commit()

    assert actor.foo == 'bar'

    assert backend.get(Actor,{'pk' : actor.pk}).foo == 'bar'

    del actor.foo

    with pytest.raises(AttributeError):
        actor.foo

    with pytest.raises(KeyError):
        actor['foo']

    backend.save(actor)
    backend.commit()

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

    for actor in actors:
        backend.save(actor)

    backend.commit()

    queryset = backend.filter(Actor, {'name': 'bar','is_funny' : True})

    assert queryset.next() == actors[0]

def test_and_queries(backend):

    backend.save(Actor({'name': 'bar', 'is_funny': False}))
    backend.save(Actor({'name': 'baz', 'is_funny': False}))
    backend.save(Actor({'name': 'baz', 'is_funny': True}))
    backend.save(Actor({'name': 'bar', 'is_funny': True}))

    backend.commit()

    assert len(backend.filter(Actor, {'name': 'bar'})) == 2
    assert len(backend.filter(Actor, {'is_funny': False})) == 2
    assert len(backend.filter(Actor, {'name': 'bar', 'is_funny': False})) == 1
    assert len(backend.filter(Actor, {'name': 'baz', 'is_funny': False})) == 1
    assert len(backend.filter(Actor, {'name': 'bar', 'is_funny': False})) == 1
    assert len(backend.filter(Actor, {'name': 'baz', 'is_funny': False})) == 1



def test_operators(backend):

    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()

    assert len(backend.filter(Actor, {})) == 4

    for op, results in (('$gt', [david_hasselhoff]), ('$gte', [david_hasselhoff]), ('$lt', [charlie_chaplin]), ('$lte', [charlie_chaplin])):

        query = {
            '$and':
            [
                {'gross_income_m': {op: 1.0}},
                {'is_funny': True}
            ]
        }

        assert len(backend.filter(Actor, query)) == len(results)
        assert results in backend.filter(Actor, query)

    for op, results in (('$gt', [david_hasselhoff, charlie_chaplin, marlon_brando]), ('$gte', [marlon_brando, david_hasselhoff, charlie_chaplin]), ('$lt', [charlie_chaplin]), ('$lte', [charlie_chaplin])):

        query = {
            '$and':
            [
                {'$or': [
                    {'gross_income_m': {op: 1.0}},
                    {'birth_year': {'$lt': 1900}},
                ]},
                {'$or': [
                    {'is_funny': True},
                    {'name': 'Marlon Brando'},
                ]},
            ]
        }

        assert len(backend.filter(Actor, query)) == len(results)
        assert results in backend.filter(Actor, query)

    assert len(backend.filter(Actor, {'name': {'$ne': 'David Hasselhoff'}})) == 3
    assert len(backend.filter(Actor, {'name': 'David Hasselhoff'})) == 1
    assert len(backend.filter(Actor, {'name': {'$not': {'$in': ['David Hasselhoff', 'Marlon Brando', 'Charlie Chaplin']}}})) == 1
    assert len(backend.filter(Actor, {'name': {'$in': ['Marlon Brando', 'Leonardo di Caprio']}})) == 2

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

    for movie in movies:
        if movie.get('director'):
            director = movie.director
            for directed_movie in backend.filter(Movie,{'director' : director}):
                backend.update(directed_movie,unset_fields = ['director'])
                backend.commit()
            backend.delete(director)

    backend.commit()

    directors = backend.filter(Director,{})

    for director in directors:
        assert len(backend.filter(Movie,{'director' : director})) == 0

def test_default_backend(backend, small_test_data):

    movies = backend.filter(Movie, {})
    old_len = len(movies)
    movie = movies[0]
    movie.delete()
    backend.commit()

    with pytest.raises(Movie.DoesNotExist):
        backend.get(Movie, {'pk': movie.pk})

    assert old_len == len(backend.filter(Movie, {})) + 1


def test_index_reloading(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    backend.filter(Actor, {'movies': movies[0]}).delete()
    backend.commit()

    assert list(backend.filter(Actor, {'movies': movies[0]})) == []
