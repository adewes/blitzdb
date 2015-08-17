from __future__ import absolute_import

from .fixtures import *

from blitzdb.tests.helpers.movie_data import Actor, Movie


def test_nested_value(backend):

    class MyMovie(Movie):

        class Meta(Movie.Meta):
            dbref_includes = ["actor.name"]

    actor = Actor({'name' : 'Robert de Niro'})
    movie = MyMovie({'actor' : actor})
    actor.movies = [movie]

    backend.save(movie)
    backend.commit()

    recovered_actor = backend.get(Actor,{'pk' : actor.pk})

    assert recovered_actor.movies[0]._lazy
    assert set(recovered_actor.movies[0].lazy_attributes.keys()) == set(['pk','actor_name'])

    assert recovered_actor.movies[0].actor_name == actor.name
    assert recovered_actor.movies[0]['actor_name'] == actor.name

    assert recovered_actor.movies[0]._lazy

    #Now we request the name attribute of the actor (which is not lazy), which triggers a revert
    assert recovered_actor.movies[0].actor.name == actor.name
    assert recovered_actor.movies[0].actor['name'] == actor.name
    assert recovered_actor.movies[0]._lazy == False

    with pytest.raises(AttributeError):
        assert recovered_actor.movies[0].actor_name

    with pytest.raises(KeyError):
        assert recovered_actor.movies[0]['actor_name']

