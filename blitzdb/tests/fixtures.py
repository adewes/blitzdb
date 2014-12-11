import pytest
import tempfile
import subprocess

try:
    import pymongo
    from blitzdb.backends.mongo import Backend as MongoBackend
    test_mongo = True

    @pytest.fixture(scope="function")
    def mongodb_backend(request):
        return mongo_backend(request, {})

    @pytest.fixture(scope="function")
    def small_mongodb_test_data(request, mongodb_backend):
        return generate_test_data(request, mongodb_backend, 20)


except ImportError:
    print("MongoDB not found, skipping tests.")
    test_mongo = False

from blitzdb.backends.file import Backend as FileBackend
from blitzdb.tests.helpers.movie_data import Actor, Director, Movie, generate_test_data


@pytest.fixture(scope="function")
def tmpdir(request):
    tmpdir = tempfile.mkdtemp()

    def finalizer():
        subprocess.call(["rm", "-rf", tmpdir])
    request.addfinalizer(finalizer)
    return tmpdir


@pytest.fixture(scope="function", params=["file_json", "file_marshal", "file_pickle"] + (["mongo"] if test_mongo else []))
def backend(request, tmpdir):
    return _backend(request, tmpdir)


@pytest.fixture(scope="function", params=["file_json", "file_marshal", "file_pickle"] + (["mongo"] if test_mongo else []))
def no_autoload_backend(request, tmpdir):
    return _backend(request, tmpdir, autoload_embedded=False)


@pytest.fixture(scope="function", params=["mongo"] if test_mongo else [])
def no_autoload_mongodb_backend(request, tmpdir):
    return _backend(request, tmpdir, autoload_embedded=False)


@pytest.fixture(scope="function", params=["file_json", "file_marshal", "file_pickle"] + (["mongo"] if test_mongo else []))
def transactional_backend(request, tmpdir):
    return _backend(request, tmpdir)


@pytest.fixture(scope="function")
def large_test_data(request, backend):
    return generate_test_data(request, backend, 100)


@pytest.fixture(scope="function")
def small_test_data(request, backend):
    return generate_test_data(request, backend, 20)


@pytest.fixture(scope="function")
def large_transactional_test_data(request, transactional_backend):
    return generate_test_data(request, transactional_backend, 100)


@pytest.fixture(scope="function")
def small_transactional_test_data(request, transactional_backend):
    return generate_test_data(request, transactional_backend, 20)


def _backend(request, tmpdir, autoload_embedded=True):
    """
    We test all query operations on a variety of backends.
    """
    if request.param == 'file_json':
        return file_backend(request, tmpdir, {'serializer_class': 'json'}, autoload_embedded=autoload_embedded)
    elif request.param == 'file_marshal':
        return file_backend(request, tmpdir, {'serializer_class': 'marshal'}, autoload_embedded=autoload_embedded)
    elif request.param == 'file_pickle':
        return file_backend(request, tmpdir, {'serializer_class': 'pickle'}, autoload_embedded=autoload_embedded)
    elif request.param == 'mongo':
        return mongo_backend(request, {}, autoload_embedded=autoload_embedded)


def _init_indexes(backend):
    for idx in [{'fields': {'name': 1}}, {'fields': {'director': 1}}]:
        backend.create_index(Movie, **idx)
    backend.create_index(Actor, fields={'name': 1})
    backend.create_index(Actor, fields={'movies': 1})
    return backend


def file_backend(request, tmpdir, config, autoload_embedded=True):
    backend = FileBackend(tmpdir, config=config, overwrite_config=True, autoload_embedded=autoload_embedded)
    _init_indexes(backend)
    return backend


def mongo_backend(request, config, autoload_embedded=True):
    con = pymongo.MongoClient()
    con.drop_database("blitzdb_test_3243213121435312431")
    db = pymongo.MongoClient()['blitzdb_test_3243213121435312431']
    backend = MongoBackend(db, autoload_embedded=autoload_embedded)
    _init_indexes(backend)
    return backend
