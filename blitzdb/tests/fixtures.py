from __future__ import print_function
import pytest
import tempfile
import subprocess
import os

from blitzdb.backends.file import Backend as FileBackend
from blitzdb.tests.helpers.movie_data import Actor, Director, Movie, Food, generate_test_data

@pytest.fixture(scope="function")
def temporary_path(request):
    d = tempfile.mkdtemp()

    def finalizer():
        subprocess.call(["rm", "-rf", d])

    request.addfinalizer(finalizer)
    return str(d)

test_mongo = False

try:
    if not os.environ.get('NO_MONGO'):
        import pymongo
        from blitzdb.backends.mongo import Backend as MongoBackend
        test_mongo = True

        @pytest.fixture(scope="function")
        def mongodb_backend(request):
            return _mongodb_backend(request, {})

        @pytest.fixture(scope="function")
        def small_mongodb_test_data(request, mongodb_backend):
            return generate_test_data(request, mongodb_backend, 20)

        print("Testing with MongoDB")
except ImportError:
    print("MongoDB not found, skipping tests.")

try:
    from sqlalchemy import create_engine, event
    from sqlalchemy.schema import MetaData
    from sqlalchemy.types import Integer
    from blitzdb.backends.sql import Backend as SqlBackend

    memory_url = 'sqlite:///:memory:'

    def get_sql_engine():
        url = os.environ.get('BLITZDB_SQLALCHEMY_URL',memory_url)
        engine = create_engine(url, echo=False)
        #we make sure foreign keys are enforced...
        return engine

    engine = get_sql_engine()

    def _sql_backend(request,engine):

        meta = MetaData(engine)
        meta.reflect()
        meta.drop_all()
        #we enable foreign key checks for SQLITE
        if str(engine.url).startswith('sqlite://'):
            engine.connect().execute('pragma foreign_keys=ON')

        backend = SqlBackend(engine = engine)
        backend.init_schema()
        backend.create_schema()

        def finalizer():
            backend.rollback()
            del backend.connection
            print("Dropping schema...")
            #we disable foreign key checks for SQLITE (as dropping tables with circular foreign keys won't work otherwise...)
            if str(engine.url).startswith('sqlite://'):
                engine.connect().execute('pragma foreign_keys=OFF')
            meta = MetaData(engine)
            meta.reflect()
            meta.drop_all()
            print("Done...")

        request.addfinalizer(finalizer)

        return backend

    @pytest.fixture(scope="function")
    def sql_backend(request):
        backend = _sql_backend(request,engine)

        return backend


    test_sql = True

    @pytest.fixture(scope="function")
    def small_sql_test_data(request, sql_backend):
        return generate_test_data(request, sql_backend, 20)

    print("Testing with SQL")
except ImportError:
    print("SQLAlchemy not found, skipping tests.")
    test_sql = False

@pytest.fixture(scope="function", params=["file_json", "file_pickle"]
                + (["mongo"] if test_mongo else [])
                + (["sql"] if test_sql else []))
def backend(request, temporary_path):
    return _backend(request, temporary_path)

@pytest.fixture(scope="function", params=["file_json", "file_pickle"]
                + (["mongo"] if test_mongo else [])
                + (["sql"] if test_sql else []))
def no_autoload_backend(request, temporary_path):
    return _backend(request, temporary_path, autoload_embedded=False)


@pytest.fixture(scope="function", params=["mongo"] if test_mongo else [])
def no_autoload_mongodb_backend(request, temporary_path):
    return _backend(request, temporary_path, autoload_embedded=False)


@pytest.fixture(scope="function", params=["file_json", "file_pickle"]
                                         + (["mongo"] if test_mongo else [])
                                         + (["sql"] if test_sql else []))
def transactional_backend(request, temporary_path):
    return _backend(request, temporary_path)


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


def _backend(request, temporary_path, autoload_embedded=True):
    """
    We test all query operations on a variety of backends.
    """
    if request.param == 'file_json':
        return _file_backend(request, temporary_path, {'serializer_class': 'json'},
                             autoload_embedded=autoload_embedded)
    elif request.param == 'file_pickle':
        return _file_backend(request, temporary_path, {'serializer_class': 'pickle'},
                             autoload_embedded=autoload_embedded)
    elif request.param == 'mongo':
        return _mongodb_backend(request, {}, autoload_embedded=autoload_embedded)
    elif request.param == 'sql':
        return _sql_backend(request, engine)


def _init_indexes(backend):
    for idx in [{'fields': {'name': 1}}, {'fields': {'director': 1}}]:
        backend.create_index(Movie, **idx)
    backend.create_index(Actor, fields={'name': 1})
    backend.create_index(Actor, fields={'movies': 1})
    return backend


def _file_backend(request, temporary_path, config, autoload_embedded=True):
    backend = FileBackend(path=temporary_path, config=config,
                          overwrite_config=True,
                          autoload_embedded=autoload_embedded)
    _init_indexes(backend)
    return backend

@pytest.fixture(scope="function")
def file_backend(request,temporary_path):
    return _file_backend(request, temporary_path,{})


def _mongodb_backend(request, config, autoload_embedded=True):
    con = pymongo.MongoClient()
    con.drop_database("blitzdb_test_3243213121435312431")
    db = pymongo.MongoClient()['blitzdb_test_3243213121435312431']
    backend = MongoBackend(db, autoload_embedded=autoload_embedded)
    _init_indexes(backend)
    return backend
