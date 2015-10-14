# -*- coding: utf-8
from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor, Director, Movie

def test_delete(backend):

    stallone = Actor({'name' : u'ßílvöster Ställöne'})
    arnie = Actor({'name' : u'Arnöld Schwürzenöggär'})

    with backend.transaction():
        backend.save(stallone)
        backend.save(arnie)

    assert backend.get(Actor,{'name' : stallone.name}) == stallone
    assert backend.get(Actor,{'name' : arnie.name}) == arnie

