from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor, Director, Movie


def test_basic_delete(backend, small_test_data):

    backend.filter(Actor, {}).delete()
    backend.commit()

    assert len(backend.filter(Actor, {})) == 0


def test_basic_storage(backend, small_test_data):

    (movies, actors, directors) = small_test_data

    assert len(backend.filter(Movie, {})) == len(movies)
    assert len(backend.filter(Actor, {})) == len(actors)


#removed this functionality since it was misleading...
@pytest.skip
def test_keys_with_dots(backend):

    actor = Actor({'some.key.with.nasty.dots': [{'some.more.nasty.dots': 100}], 'pk': 'test'})

    backend.save(actor)
    backend.commit()

    assert actor == backend.get(Actor, {'pk': 'test'})


def test_delete(backend):

    actor = Actor({'foo' : 'bar'})

    backend.save(actor)
    backend.commit()

    assert actor.foo == 'bar'

    assert backend.get(Actor,{'pk' : actor.pk}).foo == 'bar'

    del actor.foo

    with pytest.raises(AttributeError):
        actor.foo

    with pytest.raises(KeyError):
        actor['foo']

    backend.save(actor)
    backend.commit()

    with pytest.raises(AttributeError):
        backend.get(Actor,{'pk' : actor.pk}).foo

