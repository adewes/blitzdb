import pytest
import tempfile
import subprocess
import random
import time
import math
import faker #https://github.com/joke2k/faker

from blitzdb.backends.file import Backend,CompressedStore
from blitzdb import Object

class Movie(Object):
    pass

class Actor(Object):
    pass

class Role(Object):
    pass

class Director(Object):
    pass

@pytest.fixture(scope = "function")
def large_test_data(request,backend):
    return generate_test_data(request,backend,100)

@pytest.fixture(scope = "function")
def large_test_data(request,backend):
    return generate_test_data(request,backend,100)

@pytest.fixture(scope = "function")
def small_test_data(request,backend):
    return generate_test_data(request,backend,20)

def generate_test_data(request,backend,n):
    fake = faker.Faker()


    actors = []
    movies = []
    directors = []

    backend.filter(Movie,{}).delete()
    backend.filter(Actor,{}).delete()
    backend.filter(Director,{}).delete()

    for i in range(0,n):
        movie = Movie(
                {
                    'name' : fake.company(),
                    'year' : fake.year(),
                    'pk' : i,
                    'cast' : [],
                }
            )
        movies.append(movie)
        movie.save(backend)


    for i in range(0,n*4):
        actor = Actor(
            {
                'name' : fake.name(),
                'pk' : i,
                'movies' : []
            }            
            )
        n_movies = 1+int((1.0-math.log(random.randint(1,1000))/math.log(1000.0))*5)
        actor_movies = random.sample(movies,n_movies)
        for movie in actor_movies:
            actor.movies.append(movie)
            movie.cast.append({'actor':actor,'character':fake.name()})
            movie.save(backend)
        actors.append(actor)
        actor.save(backend)

    for i in range(0,n/10):
        director = Director(
                {
                    'name' : fake.name(),
                    'pk' : i,
                    'movies' : [],
                }
            )
        n_movies = 1+int((1.0-math.log(random.randint(1,1000))/math.log(1000.0))*10)
        director_movies = random.sample(movies,n_movies)
        for movie in director_movies:
            movie.director = director
            movie.save(backend)
            director.movies.append(movie)
        directors.append(director)
        director.save(backend)
    
    backend.commit()

    return (movies,actors,directors)

@pytest.fixture(scope = "module")
def tmpdir(request):

    tmpdir = tempfile.mkdtemp()
    def finalizer():
        subprocess.call(["rm","-rf",tmpdir])
    request.addfinalizer(finalizer)
    return tmpdir

@pytest.fixture(scope = "module")
def backend(request,tmpdir):
    backend = Backend(tmpdir)

    for idx in ['name','director']:
        backend.create_index(Movie,idx)

    backend.create_index(Actor,'name')
    backend.create_index(Actor,'movies')

    return backend

def test_basic_storage(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    assert len(backend.filter(Movie,{})) == len(movies)
    assert len(backend.filter(Actor,{})) == len(actors)

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

def test_indexed_delete(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    for movie in movies:
        backend.filter(Actor,{'movies' : movie}).delete()

    for actor in backend.filter(Actor,{}):
        assert actor.movies == []

def test_indexed_delete(backend):

    movie = Movie({'name' : 'The Godfather'})
    actor = Actor({'name' : 'Marlon Brando'})
    actor.performances = [movie]
    movie.cast = {'Don Corleone' : actor}

    movie.save(backend)

def test_non_indexed_delete(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    for movie in movies:
        backend.filter(Director,{'movies' : movie}).delete()

    for director in backend.filter(Director,{}):
        assert director.movies == []

def test_delete_transaction(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    movies = backend.filter(Movie,{})

    backend.begin()
    backend.filter(Movie,{}).delete()
    backend.rollback()

    assert movies == backend.filter(Movie,{})

def test_default_backend(backend,small_test_data):

    movies = backend.filter(Movie,{})
    old_len = len(movies)
    movie = movies[0]
    movie.delete()

    with pytest.raises(AttributeError):
        backend.get(Movie,{'pk' : movie.pk})

    assert old_len == len(backend.filter(Movie,{}))+1

def test_rollback_and_commit(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    backend.begin()    
    backend.filter(Movie,{}).delete()
    backend.rollback()

    assert len(set(movies)) == len(set(backend.filter(Movie,{})))
    assert sorted(movies,key = lambda x:x.pk) == sorted(backend.filter(Movie,{}),key = lambda x:x.pk)

    backend.begin()    
    backend.filter(Movie,{}).delete()
    backend.commit()

    assert 0 == len(set(backend.filter(Movie,{})))
    assert [] == sorted(backend.filter(Movie,{}),key = lambda x:x.pk)

def test_advanced_transaction(backend):

    backend.begin()

    backend.filter(Movie,{}).delete()

    movie = Movie({'name' : 'The Godfather','year' : 1979,'type' : 'US'})
    movie.save(backend)
    backend.commit()

    backend.delete(movie)
    movie.name = 'Star Wars IV'
    movie.save(backend)

    with pytest.raises(AttributeError):
        backend.get(Movie,{'name' : 'The Godfather','year' : 1979,'type' : 'US'})

    backend.rollback()

    assert backend.get(Movie,{'name' : 'The Godfather','year' : 1979}) == movie
    assert len(backend.filter(Movie,{'type':'US'})) == 1

def test_autocommit_transaction(backend):

    backend.filter(Movie,{}).delete()

    try:
        backend.autocommit = True
        movie = Movie({'name' : 'The Godfather','year' : 1979,'type' : 'US'})
        movie.save(backend)
        backend.delete(movie)
        movie.name = 'Star Wars IV'
        movie.save(backend)

        with pytest.raises(AttributeError):
            backend.get(Movie,{'name' : 'The Godfather','year' : 1979,'type' : 'US'})

        assert backend.get(Movie,{'name' : 'Star Wars IV','year' : 1979}) == movie
        assert len(backend.filter(Movie,{'type':'US'})) == 1
    finally:
        backend.autocommit = False

def test_index_reloading(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    backend.filter(Actor,{'movies' : movies[0]}).delete()
    assert list(backend.filter(Actor,{'movies' : movies[0]})) == []

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

def test_reloading(backend,tmpdir):

    reloaded_backend = Backend(tmpdir)

    for cls,params in backend.classes.items():
        reloaded_backend.register(cls,params)

    backend.commit()

    assert reloaded_backend.indexes.keys() == backend.indexes.keys()

    for collection in reloaded_backend.indexes:
        assert reloaded_backend.indexes[collection].keys() == backend.indexes[collection].keys()
        assert set([idx.key for idx in reloaded_backend.indexes[collection].values()]) == set([idx.key for idx in reloaded_backend.indexes[collection].values()])
        for index_1,index_2 in zip(backend.indexes[collection].values(),reloaded_backend.indexes[collection].values()):
            assert index_1.get_all_keys() == index_2.get_all_keys()

def test_querying_efficiency(backend,large_test_data):
    (movies,actors,directors) = large_test_data

    def benchmark_query():
        start = time.time()
        backend.filter(Movie,{'year' : 1984})
        return time.time()-start

    time_without_index = benchmark_query()

    backend.create_index(Movie,'year')

    time_with_index = benchmark_query()

    assert time_with_index*10 < time_without_index 
