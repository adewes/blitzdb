from __future__ import absolute_import


from .fixtures import *
from blitzdb.tests.helpers.movie_data import Movie


def test_delete_transaction(transactional_backend, small_transactional_test_data):

    (movies, actors, directors) = small_transactional_test_data

    all_movies = transactional_backend.filter(Movie, {})

    assert len(all_movies) == 20

    trans = transactional_backend.begin()
    transactional_backend.filter(Movie, {}).delete()

    transactional_backend.rollback(trans)

    assert all_movies == transactional_backend.filter(Movie, {})


def test_rollback_and_commit(transactional_backend, small_transactional_test_data):

    (movies, actors, directors) = small_transactional_test_data

    transactional_backend.begin()    
    transactional_backend.filter(Movie, {}).delete()
    transactional_backend.rollback()

    assert len(movies) == len(transactional_backend.filter(Movie, {}))
    assert sorted(movies, key=lambda x: x.pk) == sorted(transactional_backend.filter(Movie, {}), key=lambda x: x.pk)

    transactional_backend.begin()    
    transactional_backend.filter(Movie, {}).delete()
    transactional_backend.commit()

    assert 0 == len(set(transactional_backend.filter(Movie, {})))
    assert [] == sorted(transactional_backend.filter(Movie, {}), key=lambda x: x.pk)

def test_advanced_transaction(transactional_backend):

    transactional_backend.begin()

    transactional_backend.filter(Movie, {}).delete()
    transactional_backend.rollback()

    movie = Movie({'title': 'The Godfather', 'year': 1979})
    movie.save(transactional_backend)
    transactional_backend.commit()

    transactional_backend.begin()
    transactional_backend.delete(movie)
    movie.title = 'Star Wars IV'
    movie.save(transactional_backend)

    transactional_backend.rollback()

    assert transactional_backend.get(Movie, {'title': 'The Godfather', 'year': 1979}).title == 'The Godfather'

    assert transactional_backend.get(Movie, {'title': 'The Godfather', 'year': 1979}) == movie
    assert len(transactional_backend.filter(Movie, {'year': 1979})) == 1


def test_autocommit_transaction(transactional_backend):

    transactional_backend.filter(Movie, {}).delete()

    try:
        transactional_backend.autocommit = True
        movie = Movie({'title': 'The Godfather', 'year': 1979})
        movie.save(transactional_backend)
        transactional_backend.delete(movie)
        movie.title = 'Star Wars IV'
        movie.save(transactional_backend)

        with pytest.raises(Movie.DoesNotExist):
            transactional_backend.get(Movie, {'title': 'The Godfather', 'year': 1979})

        assert transactional_backend.get(Movie, {'title': 'Star Wars IV', 'year': 1979}) == movie
        assert len(transactional_backend.filter(Movie, {'year': 1979})) == 1
    finally:
        transactional_backend.autocommit = False
