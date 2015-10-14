from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor
from blitzdb.backends.file import Backend as FileBackend


def test_update_by_list(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    actor.name = 'Patrick Stewart'
    actor.age = 50

    with backend.transaction():
        backend.update(actor, ('name',))

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 0
    assert len(backend.filter(Actor, {'name': 'Patrick Stewart'})) == 1

    #we did not update the age field...
    assert backend.get(Actor, {'name': 'Patrick Stewart'}).age == 54


def test_update_non_indexed_field(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    actor.name = 'Patrick Stewart'
    actor.age = 50

    with backend.transaction():
        backend.update(actor, ('name','age'))

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 0
    assert len(backend.filter(Actor, {'name': 'Patrick Stewart'})) == 1

    assert backend.get(Actor, {'name': 'Patrick Stewart'}).age == 50


def test_multiple_updates(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    actor.name = 'Patrick Stewart'
    actor.age = 50

    with backend.transaction():
        backend.update(actor, ('name',))
        backend.update(actor, ('age',))

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 0
    assert len(backend.filter(Actor, {'name': 'Patrick Stewart'})) == 1

    assert backend.get(Actor, {'name' : 'Patrick Stewart'}).age == 50


def test_update_on_deleted_document_fails(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    with backend.transaction():
        backend.delete(actor)

    actor.name = 'Patrick Stewart'
    actor.age = 50

    with pytest.raises(actor.DoesNotExist):
        with backend.transaction():
            backend.update(actor, ('name',))
     

def test_update_with_dict(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    with backend.transaction():
        backend.update(actor, {'name': 'Ian McKellan'})

    assert len(backend.filter(Actor, {'name': 'Ian McKellan'})) == 1

    assert actor.name == 'Ian McKellan'
    
    with backend.transaction():
        backend.update(actor, {'name': 'Roger Moore'}, update_obj=False)

    assert len(backend.filter(Actor, {'name': 'Roger Moore'})) == 1

    assert actor.name == 'Ian McKellan'


def test_update_unset(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    with backend.transaction():
        backend.update(actor, unset_fields=['name'])

    assert len(backend.filter(Actor, {'name': 'Ian McKellan'})) == 0

    recovered_actor = backend.get(Actor, {'pk': actor.pk})

    assert recovered_actor.get('name') is None


def test_update_set_then_unset(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    with backend.transaction():
        backend.update(actor, set_fields={'name': 'Patrick Stewart'})
        backend.update(actor, unset_fields=['name'])

    assert len(backend.filter(Actor, {'name': 'Patrick Stewart'})) == 0

    recovered_actor = backend.get(Actor, {'pk': actor.pk})

    assert recovered_actor.get('name') is None


def test_update_unset_then_set(backend):

    if isinstance(backend,FileBackend):
        return

    actor = Actor({'name': 'Robert de Niro', 'age': 54})

    with backend.transaction():
        backend.save(actor)

    assert len(backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    with backend.transaction():
        backend.update(actor, unset_fields=['name'])
        backend.update(actor, set_fields={'name': 'Patrick Stewart'})

    assert len(backend.filter(Actor, {'name': 'Patrick Stewart'})) == 1

    recovered_actor = backend.get(Actor, {'pk': actor.pk})

    assert recovered_actor.name == 'Patrick Stewart'
