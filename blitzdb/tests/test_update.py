from __future__ import absolute_import

import pytest

from .fixtures import *

from blitzdb import Document
from blitzdb.tests.helpers.movie_data import Actor,Director,Movie

def test_update_by_list(mongodb_backend):

    actor = Actor({'name' : 'Robert de Niro','age' : 54})

    mongodb_backend.save(actor)
    mongodb_backend.commit()

    assert len(mongodb_backend.filter(Actor,{'name' : 'Robert de Niro'})) == 1

    actor.name = 'Patrick Stewart'
    actor.age = 50

    mongodb_backend.update(actor,('name',))

    mongodb_backend.commit()

    assert len(mongodb_backend.filter(Actor,{'name' : 'Robert de Niro'})) == 0
    assert len(mongodb_backend.filter(Actor,{'name' : 'Patrick Stewart'})) == 1

    assert len(mongodb_backend.filter(Actor,{'age' : 50})) == 0


def test_multiple_updates(mongodb_backend):

    actor = Actor({'name' : 'Robert de Niro','age' : 54})

    mongodb_backend.save(actor)
    mongodb_backend.commit()

    assert len(mongodb_backend.filter(Actor,{'name' : 'Robert de Niro'})) == 1

    actor.name = 'Patrick Stewart'
    actor.age = 50

    mongodb_backend.update(actor,('name',))
    mongodb_backend.update(actor,('age',))

    mongodb_backend.commit()

    assert len(mongodb_backend.filter(Actor,{'name' : 'Robert de Niro'})) == 0
    assert len(mongodb_backend.filter(Actor,{'name' : 'Patrick Stewart'})) == 1

    assert len(mongodb_backend.filter(Actor,{'age' : 50})) == 1


def test_update_on_deleted_document_fails(mongodb_backend):

    actor = Actor({'name' : 'Robert de Niro','age' : 54})

    mongodb_backend.save(actor)
    mongodb_backend.commit()

    assert len(mongodb_backend.filter(Actor,{'name' : 'Robert de Niro'})) == 1

    mongodb_backend.delete(actor)

    actor.name = 'Patrick Stewart'
    actor.age = 50

    with pytest.raises(actor.DoesNotExist):
        mongodb_backend.update(actor,('name',))
     

def test_update_with_invalid_dict(mongodb_backend):

    actor = Actor({'name' : 'Robert de Niro','age' : 54})

    mongodb_backend.save(actor)
    mongodb_backend.commit()

    assert len(mongodb_backend.filter(Actor,{'name' : 'Robert de Niro'})) == 1

    with pytest.raises(TypeError):
        mongodb_backend.update(actor,{'name' : 'Ian McKellan'})
     