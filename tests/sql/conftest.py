import pytest

from ..helpers.movie_data import Movie, Actor, Director, Food
from ..conftest import _sql_backend, get_sql_engine


@pytest.fixture
def backend(request):
    engine = get_sql_engine()
    backend = _sql_backend(request, engine)

    backend.register(Actor)
    backend.register(Director)
    backend.register(Movie)
    backend.register(Food)

    backend.init_schema()
    backend.create_schema()

    return backend


@pytest.fixture
def empty_backend(request):
    engine = get_sql_engine()
    backend = _sql_backend(request, engine)

    return backend
