from __future__ import absolute_import

from .fixtures import *

from .helpers.movie_data import Actor, Movie


def test_update_by_list(no_autoload_mongodb_backend):

    no_autoload_mongodb_backend.register(Movie,overwrite = True)

    actor = Actor({'name': 'Robert de Niro', 'age': 54, 'movies': [Movie({'name': 'The Godfather', 'year': 1987, 'rating': 'AAA'})]})

    no_autoload_mongodb_backend.save(actor)
    no_autoload_mongodb_backend.commit()

    assert len(no_autoload_mongodb_backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    recovered_actor = no_autoload_mongodb_backend.get(Actor, {'name': 'Robert de Niro'})

    assert recovered_actor.movies[0]._lazy == True
    assert recovered_actor.movies[0].lazy_attributes == {'year': 1987, 'pk': actor.movies[0].pk}

    with pytest.raises(AttributeError):
        recovered_actor.movies[0].rating

    assert recovered_actor.movies[0].year == 1987

    recovered_actor.movies[0].load_if_lazy()

    assert recovered_actor.movies[0].rating == 'AAA'


def test_eager_property(no_autoload_mongodb_backend):

    no_autoload_mongodb_backend.register(Movie,overwrite = True)

    actor = Actor({'name': 'Robert de Niro', 'age': 54, 'movies': [Movie({'name': 'The Godfather', 'year': 1987, 'rating': 'AAA'})]})

    no_autoload_mongodb_backend.save(actor)
    no_autoload_mongodb_backend.commit()

    assert len(no_autoload_mongodb_backend.filter(Actor, {'name': 'Robert de Niro'})) == 1

    recovered_actor = no_autoload_mongodb_backend.get(Actor, {'name': 'Robert de Niro'})

    assert recovered_actor.movies[0]._lazy == True
    assert recovered_actor.movies[0].lazy_attributes == {'year': 1987, 'pk': actor.movies[0].pk}

    with pytest.raises(AttributeError):
        recovered_actor.movies[0].rating

    assert recovered_actor.movies[0].year == 1987

    assert recovered_actor.movies[0].eager.rating == 'AAA'
