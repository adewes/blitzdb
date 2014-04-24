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

@pytest.fixture(scope = "function")
def tmpdir(request):

    tmpdir = tempfile.mkdtemp()
    def finalizer():
        subprocess.call(["rm","-rf",tmpdir])
    request.addfinalizer(finalizer)
    return tmpdir

@pytest.fixture(scope="function", params=["file_json","file_marshal","file_pickle","mongo"])
def backend(request,tmpdir):
    """
    We test all query operations on a variety of backends.
    """
    if request.param == 'file_json':
        return file_backend(request,tmpdir,{'serializer_class' : 'json'})
    elif request.param == 'file_marshal':
        return file_backend(request,tmpdir, {'serializer_class' : 'marshal'})
    elif request.param == 'file_pickle':
        return file_backend(request,tmpdir, {'serializer_class' : 'pickle'})
    elif request.param == 'mongo':
        return mongo_backend(request,{})

def _init_indexes(backend):
    for idx in ['name','director']:
        backend.create_index(Movie,idx)
    backend.create_index(Actor,'name')
    backend.create_index(Actor,'movies')
    return backend

def file_backend(request,tmpdir,config):
    backend = FileBackend(tmpdir,config = config,overwrite_config = True)
    _init_indexes(backend)
    return backend

def mongo_backend(request,config):
    con = pymongo.MongoClient()
    con.drop_database("blitzdb_test_3243213121435312431")
    db = pymongo.MongoClient()['blitzdb_test_3243213121435312431']
    backend = MongoBackend(db)
    _init_indexes(backend)
    return backend

def test_basic_storage(backend,small_test_data):

    (movies,actors,directors) = small_test_data

    assert len(backend.filter("movie",{})) == len(movies)
    assert len(backend.filter("movie",{})) == len(actors)
