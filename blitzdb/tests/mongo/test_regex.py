from ..fixtures import test_mongo
from ..helpers.movie_data import Actor,Movie

from blitzdb import Document

if test_mongo:

    from ..fixtures import mongodb_backend

    def test_regex(mongodb_backend):
        # DB setup
        backend = mongodb_backend
        backend.filter(Actor, {}).delete()

        marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
        leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': 'it depends', 'birth_year': 1974})
        david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
        charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

        backend.save(marlon_brando)
        backend.save(leonardo_di_caprio)
        backend.save(david_hasselhoff)
        backend.save(charlie_chaplin)

        backend.commit()
        assert len(backend.filter(Actor, {})) == 4
        # DB setup

        # Test with normal conditions
        query = {'name': {'$regex': 'Mar.*do'}}
        assert len(backend.filter(Actor, query)) == len([marlon_brando])
        # Test with normal conditions

        # Test with full results
        query = {'name': {'$regex': '/*'}}
        assert len(backend.filter(Actor, query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
        # Test with full results

        # Test repeating request
        query = {'$and': [{'name': {'$regex': r'^.*\s+Brando'}}, {'name': {'$regex': r'^.*\s+Brando'}}, {'name': {'$regex': r'^.*\s+Brando'}}, {'name': {'$regex': r'^.*\s+Brando'}}]}
        assert len(backend.filter(Actor, query)) == len([marlon_brando])
        # Test repeating request

        # Test with no result
        query = {'name': {'$regex': r'^test@test.com'}}
        assert len(backend.filter(Actor, query)) == len([])
        # Test with no result

        # Test with crossed type
        query = {'gross_income_m': {'$regex': r'^Marlon\s+.*$'}}
        assert len(backend.filter(Actor, query)) == len([])
        # Test with crossed type

        # Test with unknown attribute
        query = {'gross_income_bad': {'$regex': r'^Marlon\s+.*$'}}
        assert len(backend.filter(Actor, query)) == len([])
        # Test with unknown attribute
