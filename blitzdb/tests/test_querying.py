from __future__ import absolute_import

import pytest

from .fixtures import *

from blitzdb import Document
from blitzdb.tests.helpers.movie_data import Actor,Director,Movie


def test_basic_delete(backend,small_test_data):

    backend.filter(Actor,{}).delete()
    backend.commit()

    assert len(backend.filter(Actor,{})) == 0

def test_basic_storage(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    assert len(backend.filter(Movie,{})) == len(movies)
    assert len(backend.filter(Actor,{})) == len(actors)

def test_keys_with_dots(backend):

    actor = Actor({'some.key.with.nasty.dots' : [{'some.more.nasty.dots' : 100}] ,'pk' : 'test'})

    backend.save(actor)
    backend.commit()

    assert actor == backend.get(Actor,{'pk' : 'test'})


def test_negative_indexing(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    actors = backend.filter(Actor,{})

    assert actors[-1] == actors[len(actors)-1]
    assert actors[-10:-1] == actors[len(actors)-10:len(actors)-1]
    assert actors[-len(actors):-1] == actors[0:len(actors)-1]

    #To do: Make step tests for file backend (MongoDB does not support this)
#    assert actors[-10:-1:2] == actors[len(actors)-10:len(actors)-1:2]

def test_and_queries(backend):

    backend.save(Actor({'foo' : 'bar','value' : 10}))
    backend.save(Actor({'foo' : 'baz','value' : 10}))
    backend.save(Actor({'foo' : 'baz','value' : 11}))
    backend.save(Actor({'foo' : 'bar','value' : 11}))

    backend.commit()
    
    assert len(backend.filter(Actor,{'foo' : 'bar'})) == 2
    assert len(backend.filter(Actor,{'value' : 10})) == 2
    assert len(backend.filter(Actor,{'foo' : 'bar', 'value' : 10})) == 1
    assert len(backend.filter(Actor,{'foo' : 'baz', 'value' : 10})) == 1
    assert len(backend.filter(Actor,{'foo' : 'bar', 'value' : 11})) == 1
    assert len(backend.filter(Actor,{'foo' : 'baz', 'value' : 11})) == 1


def test_composite_queries(backend):

    backend.filter(Actor,{}).delete()

    backend.save(Actor({'values' : [1,2,3,4,5,6,7,8,9,10]}))
    backend.save(Actor({'values' : [7,6,5,4,3,2,1]}))
    backend.save(Actor({'values' : [1,2,3,4]}))
    backend.save(Actor({'values' : [1,2,3,4,{'foo' : 'bar'}]}))
    backend.save(Actor({'values' : 'foobar'}))

    backend.commit()

    for f in (lambda :True,lambda : backend.create_index(Actor,'values')):
    
        assert len(backend.filter(Actor,{})) == 5 
        assert len(backend.filter(Actor,{'values' : [1,2,3,4]})) == 1 
        assert len(backend.filter(Actor,{'values' : [1,2,3,4,{'foo' : 'bar'}]})) == 1 
        assert len(backend.filter(Actor,{'values' : [1,2,3,{'foo' : 'bar'},4]})) == 0 
        assert len(backend.filter(Actor,{'values' : [1,2,3,4,5]})) == 0 
        assert len(backend.filter(Actor,{'values' : [10,9,8,7,6,5,4,3,2,1]})) == 0

        assert len(backend.filter(Actor,{'values' : {'$all' : [4,3,2,1]}})) == 4
        assert len(backend.filter(Actor,{'values' : {'$all' : [4,3,2,1,{'foo' : 'bar'}]}})) == 1
        assert len(backend.filter(Actor,{'values' : {'$all' : [{'foo' : 'bar'}]}})) == 1
        assert len(backend.filter(Actor,{'values' : {'$all' : [4,3,2,1,14]}})) == 0 
        assert len(backend.filter(Actor,{'values' : {'$all' : [10,9,8,7,6,5,4,3,2,1]}})) == 1
        assert len(backend.filter(Actor,{'values' : {'$in' : [[1,2,3,4],[7,6,5,4,3,2,1],[1,2,3,5],'foobar']}})) == 3

def test_operators(backend):

    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()

    assert len(backend.filter(Actor,{})) == 4

    for op,results in (('$gt',[david_hasselhoff]),('$gte',[david_hasselhoff]),('$lt',[charlie_chaplin]),('$lte',[charlie_chaplin])):

        query = {   
                '$and' : 
                    [
                        {'gross_income_m' : { op : 1.0} },
                        {'is_funny' : True }
                    ] 
                }

        assert len(backend.filter(Actor,query)) == len(results)
        assert results in backend.filter(Actor,query)

    for op,results in (('$gt',[david_hasselhoff,charlie_chaplin,marlon_brando]),('$gte',[marlon_brando,david_hasselhoff,charlie_chaplin]),('$lt',[charlie_chaplin]),('$lte',[charlie_chaplin])):

        query = { 
                '$and' : 
                    [
                        {'$or' : [
                                    {'gross_income_m' : { op : 1.0} },
                                    {'birth_year' : { '$lt' : 1900} },
                                ]},
                        {'$or' : [
                            {'is_funny' : True },
                            {'name' : 'Marlon Brando'},
                                ]
                        },

                    ] 
                }

        assert len(backend.filter(Actor,query)) == len(results)
        assert results in backend.filter(Actor,query)

    assert len(backend.filter(Actor,{'name' : {'$ne' : 'David Hasselhoff'}})) == 3
    assert len(backend.filter(Actor,{'name' : 'David Hasselhoff'})) == 1
    assert len(backend.filter(Actor,{'name' : {'$not' : {'$in' : ['David Hasselhoff','Marlon Brando','Charlie Chaplin']}}})) == 1
    assert len(backend.filter(Actor,{'name' : {'$in' : ['Marlon Brando','Leonardo di Caprio']}})) == 2

def test_regex_operator(backend,small_test_data):

    backend.filter(Actor,{}).delete()
    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    marlon_wayans = Actor({'name' : 'Marlon Wayans'})
    backend.save(marlon_brando)
    backend.save(marlon_wayans)
    backend.commit()

    assert backend.get(Actor,{'name' : {'$regex' : r'^Marlon\s+(?!Wayans)[\w]+$'}}) == marlon_brando
    assert len(backend.filter(Actor,{'name' : {'$regex' : r'^Marlon\s+.*$'}})) == 2
    assert len(backend.filter(Actor,{'name' : {'$regex' : r'^.*\s+Brando$'}})) == 1


def test_list_query(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    movie = None
    i = 0
    while not movie or len(movie.cast) < 4:
        movie = movies[i]
        actor = movie.cast[0]['actor']
        i+=1

    other_movie = movies[i%len(movies)]
    while other_movie in actor.movies:
        other_movie = movies[i%len(movies)]
        i+=1

    assert actor in backend.filter(Actor,{'movies' : movie})
    assert actor not in backend.filter(Actor,{'movies' : other_movie})

def test_list_query_multiple_items(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    actor = None
    i = 0
    while not actor or len(actor.movies) < 2:
        actor = actors[i]
        i+=1

    assert actor in backend.filter(Actor,{'movies' : actor.movies})


def test_indexed_delete(backend,small_test_data):

    all_movies = backend.filter(Movie,{})

    for movie in all_movies:
        backend.filter(Actor,{'movies' : movie}).delete()

    backend.commit()

    for actor in backend.filter(Actor,{}):
        assert actor.movies == []

def test_non_indexed_delete(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    for movie in movies:
        backend.filter(Director,{'movies' : {'$all' : [movie]} }).delete()

    backend.commit()

    for director in backend.filter(Director,{}):
        assert director.movies == []

def test_positional_query(backend,small_test_data):

    """
    We test a search query which explicitly references a given list item in an object
    """

    (movies,actors,directors) = small_test_data

    movie = None
    i = 0
    while not movie or len(movie.cast) < 3:
        if len(movies[i].cast):
            movie = movies[i]
            actor = movie.cast[0]['actor']
            index = actor.movies.index(movie)
        i+=1

    assert actor in backend.filter(Actor,{'movies.%d' % index : movie})

def test_default_backend(backend,small_test_data):

    movies = backend.filter(Movie,{})
    old_len = len(movies)
    movie = movies[0]
    movie.delete()
    backend.commit()

    with pytest.raises(Movie.DoesNotExist):
        backend.get(Movie,{'pk' : movie.pk})

    assert old_len == len(backend.filter(Movie,{}))+1

def test_index_reloading(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    backend.filter(Actor,{'movies' : movies[0]}).delete()
    backend.commit()

    assert list(backend.filter(Actor,{'movies' : movies[0]})) == []
