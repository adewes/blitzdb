from blitzdb.backends.sql import Backend
from blitzdb.fields import ForeignKeyField
from blitzdb.tests.helpers.movie_data import Movie,Actor,Director
from sqlalchemy import create_engine

import pytest

class MyMovie(Movie):

    best_actor = ForeignKeyField(Actor,backref = 'best_movies')
    director = ForeignKeyField(Director,backref = 'my_movies')

@pytest.fixture(scope="function")
def backend():

    engine = create_engine('sqlite:///:memory:', echo=False)
    backend = Backend(engine = engine,autodiscover_classes = False)
    backend.register(MyMovie)
    backend.register(Actor)
    backend.register(Director)
    backend.register(Movie)

    backend.init_schema()
    backend.create_schema()

    return backend
