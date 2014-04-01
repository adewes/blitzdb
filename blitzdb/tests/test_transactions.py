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
def large_test_data(request,transactional_backend):
    return generate_test_data(request,transactional_backend,100)

@pytest.fixture(scope = "function")
def small_test_data(request,transactional_backend):
    return generate_test_data(request,transactional_backend,20)

@pytest.fixture(scope = "module")
def tmpdir(request):

    tmpdir = tempfile.mkdtemp()
    def finalizer():
        subprocess.call(["rm","-rf",tmpdir])
    request.addfinalizer(finalizer)
    return tmpdir

@pytest.fixture(scope="function", params=["file","mongo"])
def transactional_backend(request,tmpdir):
    if request.param == 'file':
        return file_backend(request,tmpdir)
    elif request.param == 'mongo':
        return mongo_backend(request)

def mongo_backend(request):
    con = pymongo.MongoClient()
    con.drop_database("blitzdb_test_3243213121435312431")
    db = pymongo.MongoClient()['blitzdb_test_3243213121435312431']
    backend = MongoBackend(db)
    _init_indexes(backend)
    return backend

@pytest.fixture(scope="function")
def file_backend(request,tmpdir):
    backend = FileBackend(tmpdir)
    _init_indexes(backend)
    return backend

def _init_indexes(backend):
    for idx in ['name','director']:
        backend.create_index(Movie,idx)
    backend.create_index(Actor,'name')
    backend.create_index(Actor,'movies')
    return backend

def test_delete_transaction(transactional_backend,small_test_data):

    (movies,actors,directors) = small_test_data

    all_movies = transactional_backend.filter(Movie,{})

    transactional_backend.begin()
    transactional_backend.filter(Movie,{}).delete()

    assert len(transactional_backend.filter(Movie,{})) == len(movies)

    transactional_backend.rollback()

    assert all_movies == transactional_backend.filter(Movie,{})

def test_rollback_and_commit(transactional_backend,small_test_data):

    (movies,actors,directors) = small_test_data

    transactional_backend.begin()    
    transactional_backend.filter(Movie,{}).delete()
    transactional_backend.rollback()

    assert len(set(movies)) == len(set(transactional_backend.filter(Movie,{})))
    assert sorted(movies,key = lambda x:x.pk) == sorted(transactional_backend.filter(Movie,{}),key = lambda x:x.pk)

    transactional_backend.begin()    
    transactional_backend.filter(Movie,{}).delete()
    transactional_backend.commit()

    assert 0 == len(set(transactional_backend.filter(Movie,{})))
    assert [] == sorted(transactional_backend.filter(Movie,{}),key = lambda x:x.pk)

def test_advanced_transaction(transactional_backend):

    transactional_backend.begin()

    transactional_backend.filter(Movie,{}).delete()

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

