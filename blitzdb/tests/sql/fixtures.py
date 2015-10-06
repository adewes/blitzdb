from blitzdb.backends.sql import Backend
from blitzdb.fields import ForeignKeyField
from blitzdb.tests.helpers.movie_data import Movie,Actor,Director,Food
from sqlalchemy import create_engine
from ..fixtures import _sql_backend,get_sql_engine

import pytest

@pytest.fixture(scope="function")
def backend(request):

    engine = get_sql_engine()
    backend = _sql_backend(request,engine)

    backend.register(Actor)
    backend.register(Director)
    backend.register(Movie)
    backend.register(Food)

    backend.init_schema()
    backend.create_schema()

    return backend

@pytest.fixture(scope="function")
def empty_backend(request):

    engine = get_sql_engine()
    backend = _sql_backend(request,engine)

    return backend
