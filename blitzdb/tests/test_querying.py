import pytest
import tempfile
import subprocess
import random
import time
import pymongo

from blitzdb.backends.mongo import Backend as MongoBackend
from blitzdb.backends.file import Backend as FileBackend
from blitzdb import Document
from blitzdb.tests.helpers.movie_data import Actor,Director,Movie,generate_test_data

@pytest.fixture(scope = "function")
def large_test_data(request,backend):
    return generate_test_data(request,backend,100)

@pytest.fixture(scope = "function")
def small_test_data(request,backend):
    return generate_test_data(request,backend,20)

@pytest.fixture(scope = "module")
def tmpdir(request):

    tmpdir = tempfile.mkdtemp()
    def finalizer():
        subprocess.call(["rm","-rf",tmpdir])
    request.addfinalizer(finalizer)
    return tmpdir

@pytest.fixture(scope="function", params=["file", "mongo"])
def backend(request,tmpdir):
    if request.param == 'file':
        return file_backend(request,tmpdir)
    elif request.param == 'mongo':
        return mongo_backend(request)

def _init_indexes(backend):
    for idx in ['name','director']:
        backend.create_index(Movie,idx)
    backend.create_index(Actor,'name')
    backend.create_index(Actor,'movies')
    return backend

@pytest.fixture(scope="function")
def file_backend(request,tmpdir):
    backend = FileBackend(tmpdir)
    _init_indexes(backend)
    return backend

@pytest.fixture(scope="function")
def mongo_backend(request):
    con = pymongo.MongoClient()
    con.drop_database("blitzdb_test_3243213121435312431")
    db = pymongo.MongoClient()['blitzdb_test_3243213121435312431']
    backend = MongoBackend(db)
    _init_indexes(backend)
    return backend

def test_basic_delete(backend,small_test_data):

    backend.filter(Actor,{}).delete()

    assert len(backend.filter(Actor,{})) == 0

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

    all_movies = backend.filter(Movie,{})

    for movie in all_movies:
        backend.filter(Actor,{'movies' : movie}).delete()

    for actor in backend.filter(Actor,{}):
        assert actor.movies == []

def test_non_indexed_delete(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    for movie in movies:
        backend.filter(Director,{'movies' : movie}).delete()

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

    with pytest.raises(Movie.DoesNotExist):
        backend.get(Movie,{'pk' : movie.pk})

    assert old_len == len(backend.filter(Movie,{}))+1

def test_index_reloading(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    backend.filter(Actor,{'movies' : movies[0]}).delete()
    assert list(backend.filter(Actor,{'movies' : movies[0]})) == []

def test_reloading_file_backend(file_backend,tmpdir):

    reloaded_backend = FileBackend(tmpdir)

    for cls,params in file_backend.classes.items():
        reloaded_backend.register(cls,params)

    assert reloaded_backend.indexes.keys() == file_backend.indexes.keys()

    for collection in reloaded_backend.indexes:
        assert reloaded_backend.indexes[collection].keys() == file_backend.indexes[collection].keys()
        assert set([idx.key for idx in reloaded_backend.indexes[collection].values()]) == set([idx.key for idx in reloaded_backend.indexes[collection].values()])
        for index_1,index_2 in zip(file_backend.indexes[collection].values(),reloaded_backend.indexes[collection].values()):
            assert set(index_1.get_all_keys()) == set(index_2.get_all_keys())

def test_querying_efficiency(backend,large_test_data):

    if not isinstance(backend,FileBackend):
        return

    def benchmark_query():
        start = time.time()
        backend.filter(Movie,{'year' : 1984})
        return time.time()-start

    time_without_index = benchmark_query()
    backend.create_index(Movie,'year')
    time_with_index = benchmark_query()

    assert time_with_index < time_without_index 
