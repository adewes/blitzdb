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

def test_basic_sorting(backend):

    backend.filter(Actor,{}).delete()

    backend.save(Actor({'birth_year' : 1983}))
    backend.save(Actor({'birth_year' : 1983}))
    backend.save(Actor({'birth_year' : 1984}))
    backend.save(Actor({'birth_year' : 1984}))
    backend.save(Actor({'birth_year' : 1984}))
    backend.save(Actor({'birth_year' : 1985}))
    backend.save(Actor({'birth_year' : 1980}))
    backend.save(Actor({'birth_year' : 1990}))
    backend.save(Actor({'birth_year' : 2000}))
    backend.save(Actor({'birth_year' : 2000}))
    backend.save(Actor({'birth_year' : 1900}))
    backend.save(Actor({'birth_year' : 1843}))
    backend.save(Actor({'birth_year' : 2014}))

    backend.commit()

    actors = backend.filter(Actor,{}).sort('birth_year')
    for i in range(1,len(actors)):
        assert actors[i-1].birth_year <= actors[i].birth_year

    actors = backend.filter(Actor,{}).sort('birth_year',-1)
    for i in range(1,len(actors)):
        assert actors[i-1].birth_year >= actors[i].birth_year

    actors = backend.filter(Actor,{}).sort([('birth_year',-1)])
    for i in range(1,len(actors)):
        assert actors[i-1].birth_year >= actors[i].birth_year

    """
    Objects with missing sort keys should be returned first when
    sorting in ascending order, else last.
    """

    actor_wo_birth_year = Actor({})

    backend.save(actor_wo_birth_year)
    backend.commit()
    actors = backend.filter(Actor,{}).sort([('birth_year',1)])
    assert actors[0] == actor_wo_birth_year

