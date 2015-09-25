from .fixtures import *


def test_in(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with empty list
    query = {'name': {'$not': {'$in': []}}}
    assert len(backend.filter(Actor, query)) == len([david_hasselhoff, charlie_chaplin, marlon_brando, leonardo_di_caprio])
    query = {'name': {'$in': []}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with empty list

    # Test with one match
    query = {'name': {'$in': [david_hasselhoff.name]}}
    assert len(backend.filter(Actor, query)) == len([david_hasselhoff])
    # Test with one match

    # Test with unknown elements
    query = {'name': {'$in': ['jackie chan']}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with unknown elements

    # Test with different types
    query = {'name': {'$in': [david_hasselhoff.name]}}
    assert len(backend.filter(Actor, query)) == len([david_hasselhoff])
    # Test with different types


def test_lt(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with String
    query = {'name': {'$lt': marlon_brando.name}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    # Test with String

    # Test with float/int
    query = {'gross_income_m': {'$lt': marlon_brando.appearances}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin, leonardo_di_caprio, david_hasselhoff])
    # Test with float/int

    # Test with normal conditions
    query = {'appearances': {'$lt': david_hasselhoff.appearances}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, leonardo_di_caprio])
    # Test with normal conditions

    # Test with normal conditions
    query = {'gross_income_m': {'$lt': david_hasselhoff.gross_income_m}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin])
    # Test with normal conditions


def test_gt(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with String
    query = {'name': {'$gt': marlon_brando.name}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with String

    # Test with float/int
    query = {'gross_income_m': {'$gt': marlon_brando.appearances}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with float/int

    # Test with normal conditions
    query = {'appearances': {'$gt': david_hasselhoff.appearances}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test with normal conditions

    # Test with normal conditions
    query = {'gross_income_m': {'$gt': marlon_brando.gross_income_m}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, david_hasselhoff])
    # Test with normal conditions


def test_gte(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with String
    query = {'name': {'$gte': marlon_brando.name}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando])
    # Test with String

    # Test with float/int
    query = {'gross_income_m': {'$gte': marlon_brando.appearances}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with float/int

    # Test with normal conditions
    query = {'appearances': {'$gte': david_hasselhoff.appearances}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, marlon_brando])
    # Test with normal conditions

    # Test with normal conditions
    query = {'gross_income_m': {'$gte': marlon_brando.gross_income_m}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff])
    # Test with normal conditions


def test_lte(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with String
    query = {'name': {'$lte': marlon_brando.name}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
    # Test with String

    # Test with float/int
    query = {'gross_income_m': {'$lte': marlon_brando.appearances}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin, leonardo_di_caprio, david_hasselhoff])
    # Test with float/int

    # Test with normal conditions
    query = {'appearances': {'$lte': david_hasselhoff.appearances}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, leonardo_di_caprio, david_hasselhoff])
    # Test with normal conditions

    # Test with normal conditions
    query = {'gross_income_m': {'$lte': david_hasselhoff.gross_income_m}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    # Test with normal conditions


def test_exists(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34 ,'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with normal conditions
    query = {'is_funny': {'$exists': True}}
    assert len(backend.filter(Actor, query)) == 2
    assert all([actor in backend.filter(Actor, query) for actor in [marlon_brando,charlie_chaplin]])

    query = {'is_funny': {'$exists': False}}
    assert len(backend.filter(Actor, query)) == 2
    assert all([actor in backend.filter(Actor, query) for actor in [leonardo_di_caprio,david_hasselhoff]])


def test_all(backend):
    # DB setup
    #currently those queries are not supported by the file backend.
    if isinstance(backend,FileBackend):
        return
    backend.filter(Actor, {}).delete()

    pizza = Food({'name' : 'Pizza'})
    spaghetti = Food({'name' : 'Spaghetti'})
    focaccia = Food({'name' : 'Foccacia'})
    hamburger = Food({'name' : 'Hamburger'})
    weisswurst = Food({'name' : 'Weisswurst'})
    oysters = Food({'name' : 'oysters'})

    marlon_brando = Actor({'name': 'Marlon Brando','favorite_food' : [pizza,spaghetti,focaccia], 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'favorite_food' : [hamburger,pizza,spaghetti], 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff','favorite_food' : [hamburger,weisswurst], 'gross_income_m': 1.0, 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'favorite_food' : [oysters,spaghetti],'gross_income_m': 0.371, 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)
    #contained nowhere
    backend.save(focaccia)

    backend.commit()

    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with float
    query = {'favorite_food': {'$all': [pizza, spaghetti,focaccia]}}
    assert len(backend.filter(Actor, query)) == 1
    # Test with float

    # Test with full result
    query = {'favorite_food': {'$all': [pizza,spaghetti]}}
    assert len(backend.filter(Actor, query)) == 2
    # Test with full result

    # Test with full result
    query = {'favorite_food': {'$all': [spaghetti]}}
    assert len(backend.filter(Actor, query)) == 3
    # Test with full result


def test_ne(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
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
    query = {'name': {'$ne': charlie_chaplin.name}}
    assert len(backend.filter(Actor, query)) == 3
    # Test with normal conditions

    # Test with no result
    query = {'name': {'$ne': 'jackie chan'}}
    assert len(backend.filter(Actor, query)) == 4
    # Test with no result

    # Test with int
    query = {'appearances': {'$ne': 78}}
    assert len(backend.filter(Actor, query)) == 3
    # Test with int

    # Test with float/full results
    query = {'gross_income_m': {'$ne': 0.0}}
    assert len(backend.filter(Actor, query)) == 4
    # Test with float/full results

    # Test with boolean
    query = {'is_funny': {'$ne': True}}
    assert len(backend.filter(Actor, query)) == 2
    # Test with boolean

    # Test with boolean/string
    query = {'is_funny': {'$ne': False}}
    assert len(backend.filter(Actor, query)) == 2
    # Test with boolean/string

    # Test with crossed type
    query = {'appearances': {'$ne': 111}}
    assert len(backend.filter(Actor, query)) == 4
    # Test with crossed type


def test_and(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
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
    query = {'$and': [{'name': charlie_chaplin.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == 1
    # Test with normal conditions

    # Test with no results
    query = {'$and': [{'name': charlie_chaplin.name}, {'birth_year': 1924}, {'is_funny': False}, {'gross_income_m': '12.453'}]}
    assert len(backend.filter(Actor, query)) == 0
    # Test with no results

    # Test repeating request
    query = {'$and': [{'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}]}
    assert len(backend.filter(Actor, query)) == 1
    # Test repeating request

    # Test with no result
    query = {'$and': [{'name': charlie_chaplin.name}, {'birth_year': {'$lt': 1889}}]}
    assert len(backend.filter(Actor, query)) == 0
    # Test with no result

    # Test with no result
    query = {'$and': [{'appearances': 473}, {'birth_year': {'$lt': 1879}}]}
    assert len(backend.filter(Actor, query)) == 0
    # Test with no result

    # Test with crossed type
    query = {'$and': [{'name': charlie_chaplin.appearances}, {'birth_year': 1942}]}
    assert len(backend.filter(Actor, query)) == 0
    # Test with crossed type


def test_or(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': False, 'birth_year': 1974})
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
    query = {'$or': [{'name': charlie_chaplin.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test with normal conditions

    # Test with full results
    query = {'$or': [{'name': charlie_chaplin.name}, {'birth_year': 1924}, {'is_funny': False}, {'gross_income_m': 12.453}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
    # Test with full results

    # Test repeating request
    query = {'$or': [{'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test repeating request


