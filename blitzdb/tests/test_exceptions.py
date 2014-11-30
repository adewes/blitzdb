import pytest

from blitzdb.tests.helpers.movie_data import Actor, Director


def test_basic_exceptions():
    """
    We test that the basic exceptions work as expected.
    """

    try:
        raise Actor.DoesNotExist
    except Actor.DoesNotExist:
        pass

    try:
        raise Actor.MultipleDocumentsReturned
    except Actor.MultipleDocumentsReturned:
        pass


def test_exception_distinguishability():
    """
    We test that exceptions belonging to different base classes are indeed different
    """

    with pytest.raises(Actor.DoesNotExist):
        try:
            raise Actor.DoesNotExist
        except Director.DoesNotExist:
            pass

    with pytest.raises(Actor.MultipleDocumentsReturned):
        try:
            raise Actor.MultipleDocumentsReturned
        except Director.MultipleDocumentsReturned:
            pass
