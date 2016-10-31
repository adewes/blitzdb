import pytest
import pprint
import datetime

from blitzdb.backends.sql import Backend
from blitzdb.fields import CharField, ForeignKeyField, ManyToManyField
from blitzdb import Document
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import String
from ..fixtures import _sql_backend, get_sql_engine

class DirectorAward(Document):

    class Meta(Document.Meta):
        autoregister = False

    name = CharField(indexed=True)

    director = ForeignKeyField('Director', backref='awards')

class Actor(Document):

    class Meta(Document.Meta):
        autoregister = False

    name = CharField(indexed=True)

class Movie(Document):

    class Meta(Document.Meta):
        autoregister = False

    director = ForeignKeyField('Director', backref='movies')
    actors = ManyToManyField('Actor', backref='movies')
    name = CharField(indexed=True)

class Director(Document):

    class Meta(Document.Meta):
        autoregister = False

    name = CharField(indexed=True)

def _init_backend(backend):
    backend.register(Actor)
    backend.register(Movie)
    backend.register(Director)
    backend.register(DirectorAward)
    backend.init_schema()
    backend.create_schema()

    ted_kotcheff = Director({'name' : 'Ted Kotcheff'})
    silvester_stallone = Actor({'name' : 'Silvester Stallone'})
    rambo = Movie({'name' : 'Rambo I','actors' : [silvester_stallone],'director' : ted_kotcheff})
    oscar = DirectorAward({'name' : 'Oscar', 'director' : ted_kotcheff})

    with backend.transaction():
        backend.save(rambo)
        backend.save(oscar)


@pytest.fixture(scope="function")
def cascade_backend(request):
    engine = get_sql_engine()
    backend = _sql_backend(request,engine, autodiscover_classes = False, ondelete='CASCADE')
    _init_backend(backend)
    return backend

@pytest.fixture(scope="function")
def nocascade_backend(request):
    engine = get_sql_engine()
    backend = _sql_backend(request,engine, autodiscover_classes = False, ondelete=None)
    _init_backend(backend)
    return backend

def test_foreign_key_delete_cascade(cascade_backend):

    movie = cascade_backend.get(Movie,{})
    director = cascade_backend.get(Director,{})

    director.delete()

    assert cascade_backend.filter(Actor,{})
    assert not cascade_backend.filter(Director,{})
    assert not cascade_backend.filter(Movie,{})
    assert not cascade_backend.filter(DirectorAward,{})

def test_foreign_key_delete_nocascade(nocascade_backend):

    movie = nocascade_backend.get(Movie,{})
    actor = nocascade_backend.get(Actor,{})
    director = nocascade_backend.get(Director,{})

    with pytest.raises(IntegrityError):
        director.delete()

    assert actor in nocascade_backend.filter(Actor,{})
    assert director in nocascade_backend.filter(Director,{})
    assert movie in nocascade_backend.filter(Movie,{})

def test_many_to_many_delete_cascade(cascade_backend):

    movie = cascade_backend.get(Movie,{})
    actor = cascade_backend.get(Actor,{})

    actor.delete()

    assert not cascade_backend.filter(Actor,{})
    assert cascade_backend.filter(Movie,{})


def test_many_to_many_delete_nocascade(nocascade_backend):

    movie = nocascade_backend.get(Movie,{})
    actor = nocascade_backend.get(Actor,{})
    director = nocascade_backend.get(Director,{})

    with pytest.raises(IntegrityError):
        actor.delete()

    assert actor in nocascade_backend.filter(Actor,{})
    assert director in nocascade_backend.filter(Director,{})
    assert movie in nocascade_backend.filter(Movie,{})
