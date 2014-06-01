from __future__ import absolute_import

from .fixtures import *

from blitzdb import Document
from blitzdb.tests.helpers.movie_data import Actor,Director,Movie

def test_basic_sorting(backend):

    backend.filter(Actor,{}).delete()

    backend.save(Actor({'birth_year' : 1983}))
    backend.save(Actor({'birth_year' : 1983}))
    backend.save(Actor({'birth_year' : 1984}))
    backend.save(Actor({'birth_year' : 1984}))
    backend.save(Actor({'birth_year' : 1984}))
    backend.save(Actor({'birth_year' : 1985}))
    backend.save(Actor({'birth_year' : 1980}))
    backend.save(Actor({'birth_year' : 1990}))
    backend.save(Actor({'birth_year' : 2000}))
    backend.save(Actor({'birth_year' : 2000}))
    backend.save(Actor({'birth_year' : 1900}))
    backend.save(Actor({'birth_year' : 1843}))
    backend.save(Actor({'birth_year' : 2014}))

    backend.commit()

    actors = backend.filter(Actor,{}).sort('birth_year')
    for i in range(1,len(actors)):
        assert actors[i-1].birth_year <= actors[i].birth_year

    actors = backend.filter(Actor,{}).sort('birth_year',-1)
    for i in range(1,len(actors)):
        assert actors[i-1].birth_year >= actors[i].birth_year

    actors = backend.filter(Actor,{}).sort([('birth_year',-1)])
    for i in range(1,len(actors)):
        assert actors[i-1].birth_year >= actors[i].birth_year

    """
    Objects with missing sort keys should be returned first when
    sorting in ascending order, else last.
    """

    actor_wo_birth_year = Actor({})

    backend.save(actor_wo_birth_year)
    backend.commit()
    actors = backend.filter(Actor,{}).sort([('birth_year',1)])
    assert actors[0] == actor_wo_birth_year

