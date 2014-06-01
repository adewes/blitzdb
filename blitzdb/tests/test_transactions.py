from __future__ import absolute_import

import pytest

from .fixtures import *
from blitzdb import Document
from blitzdb.tests.helpers.movie_data import Actor,Director,Movie

def test_delete_transaction(transactional_backend,small_transactional_test_data):

    (movies,actors,directors) = small_transactional_test_data

    all_movies = transactional_backend.filter(Movie,{})

    transactional_backend.begin()
    transactional_backend.filter(Movie,{}).delete()

    assert len(transactional_backend.filter(Movie,{})) == len(all_movies)

    transactional_backend.rollback()

    assert all_movies == transactional_backend.filter(Movie,{})

def test_rollback_and_commit(transactional_backend,small_transactional_test_data):

    (movies,actors,directors) = small_transactional_test_data

    transactional_backend.begin()    
    transactional_backend.filter(Movie,{}).delete()
    transactional_backend.rollback()

    assert len(movies) == len(transactional_backend.filter(Movie,{}))
    assert sorted(movies,key = lambda x:x.pk) == sorted(transactional_backend.filter(Movie,{}),key = lambda x:x.pk)

    transactional_backend.begin()    
    transactional_backend.filter(Movie,{}).delete()
    transactional_backend.commit()

    assert 0 == len(set(transactional_backend.filter(Movie,{})))
    assert [] == sorted(transactional_backend.filter(Movie,{}),key = lambda x:x.pk)

def test_advanced_transaction(transactional_backend):

    transactional_backend.begin()

    transactional_backend.filter(Movie,{}).delete()
    transactional_backend.rollback()

    movie = Movie({'name' : 'The Godfather','year' : 1979,'type' : 'US'})
    movie.save(transactional_backend)
    transactional_backend.commit()

    transactional_backend.delete(movie)
    movie.name = 'Star Wars IV'
    movie.save(transactional_backend)

    transactional_backend.rollback()

    assert transactional_backend.get(Movie,{'name' : 'The Godfather','year' : 1979,'type' : 'US'}).name == 'The Godfather'

    assert transactional_backend.get(Movie,{'name' : 'The Godfather','year' : 1979}) == movie
    assert len(transactional_backend.filter(Movie,{'type':'US'})) == 1

def test_autocommit_transaction(transactional_backend):

    transactional_backend.filter(Movie,{}).delete()

    try:
        transactional_backend.autocommit = True
        movie = Movie({'name' : 'The Godfather','year' : 1979,'type' : 'US'})
        movie.save(transactional_backend)
        transactional_backend.delete(movie)
        movie.name = 'Star Wars IV'
        movie.save(transactional_backend)

        with pytest.raises(Movie.DoesNotExist):
            transactional_backend.get(Movie,{'name' : 'The Godfather','year' : 1979,'type' : 'US'})

        assert transactional_backend.get(Movie,{'name' : 'Star Wars IV','year' : 1979}) == movie
        assert len(transactional_backend.filter(Movie,{'type':'US'})) == 1
    finally:
        transactional_backend.autocommit = False

