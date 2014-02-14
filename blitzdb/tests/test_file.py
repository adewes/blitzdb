import pytest
import tempfile
import subprocess
import random
import time
import math
import faker #https://github.com/joke2k/faker

from blitzdb.backends.file import Backend
from blitzdb import Object

class Movie(Object):
    pass

class Actor(Object):
    pass

class Role(Object):
    pass

class Director(Object):
    pass

@pytest.fixture(scope = "module")
def large_test_data(request,backend):
    return generate_test_data(request,backend,100)

@pytest.fixture(scope = "module")
def small_test_data(request,backend):
    return generate_test_data(request,backend,100)

def generate_test_data(request,backend,n):
    fake = faker.Faker()

    actors = []
    movies = []
    directors = []

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
        n_movies = 1+int((1.0-math.log(random.randint(1,1000))/math.log(1000.0))*20)
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
                    'pk' : i
                }
            )
        n_movies = 1+int((1.0-math.log(random.randint(1,1000))/math.log(1000.0))*20)
        director_movies = random.sample(movies,n_movies)
        for movie in director_movies:
            movie.director = director
            movie.save(backend)
        directors.append(director)
        director.save(backend)
    
    return (movies,actors,directors)

@pytest.fixture(scope = "module")
def tmpdir(request):

    tmpdir = tempfile.mkdtemp()
    def finalizer():
        print subprocess.check_output(["du","-h",tmpdir])
#        subprocess.call(["rm","-rf",tmpdir])
    request.addfinalizer(finalizer)
    return tmpdir

@pytest.fixture(scope = "module")
def backend(request,tmpdir):
    backend = Backend(tmpdir)
    backend.register(Movie,{'collection' : 'movies'})
    backend.register(Actor,{'collection' : 'actors'})
    backend.register(Director,{'collection' : 'directors'})
    for idx in ['name','year','director']:
        backend.create_index(Movie,idx)
    backend.create_index(Actor,'name')
    backend.create_index(Actor,'movies')

    def finalizer():
        backend.save_indexes()
    request.addfinalizer(finalizer)

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

    assert actor in backend.filter(Actor,{'movies' : movie})
    assert actor not in backend.filter(Actor,{'movies' : other_movie})

def test_indexed_query(backend,small_test_data):

    """
    We test a search query which explicitly references a given list item in an object
    """

    (movies,actors,directors) = small_test_data

    movie = None
    i = 0
    while not movie or len(movie.cast) < 2:
        movie = movies[i]
        actor = movie.cast[0]['actor']
        index = actor.movies.index(movie)
        if index == len(actor.movies)-1:
            movie = None
        i+=1

    assert actor in backend.filter(Actor,{'movies.%d' % index : movie})
    assert actor not in backend.filter(Actor,{'movies.%d' % (index+1) : movie})

def test_querying_efficiency(backend,large_test_data):
    (movies,actors,directors) = large_test_data
    start = time.time()
    backend.filter(Movie,{'year' : 1984})
    assert time.time()-start < 1e-4
    print actors[0]
