from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor, Director, Movie

def test_delete(backend):

    stallone = Actor({'name' : 'Silvester Stallone'})
    arnie = Actor({'name' : 'Arnold Schwarzenegger'})

    backend.save(stallone)
    backend.save(arnie)
    backend.commit()

    assert backend.get(Actor,{'name' : 'Silvester Stallone'}) == stallone
    assert backend.get(Actor,{'name' : 'Arnold Schwarzenegger'}) == arnie

    with pytest.raises(Actor.DoesNotExist):
        actor = backend.get(Actor,{'name' : 'Eddie Murphy'})

    with pytest.raises(Actor.MultipleDocumentsReturned):
        actor = backend.get(Actor,{})

