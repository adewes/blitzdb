from blitzdb.backends.sql import Backend
from blitzdb.fields import ForeignKeyField
from blitzdb.tests.helpers.movie_data import Movie,Actor,Director,Food
from sqlalchemy import create_engine
from ..fixtures import get_sql_engine

import pytest


@pytest.fixture(scope="function")
def backend():

    engine = get_sql_engine()
    backend = Backend(engine = engine,autodiscover_classes = False)
    backend.register(Actor)
    backend.register(Director)
    backend.register(Movie)
    backend.register(Food)

    backend.init_schema()
    backend.create_schema()

    return backend
