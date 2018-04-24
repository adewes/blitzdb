from __future__ import absolute_import

from .helpers.movie_data import Movie


def test_basic_storage(backend):

    #the dots will get encoded and should be decoded when loading the data...
    movie = Movie({"foo.bar.baz" : "bar"})

    with backend.transaction():
        backend.save(movie)

    recovered_movie = backend.get(Movie,{})

    assert 'foo.bar.baz' in recovered_movie and recovered_movie['foo.bar.baz'] == 'bar'
