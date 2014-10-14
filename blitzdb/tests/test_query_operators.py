import pytest
import tempfile
import subprocess
import random
import time
import math
from collections import Counter

from blitzdb.tests.helpers.movie_data import Actor, Director, Movie, generate_test_data

from .fixtures import *

jackie_chan = Actor({'name': 'Jackie Chan', 'gross_income_m': 3.456, 'appearances': 122, 'is_funny': True, 'birth_year': 1954})


def test_regex_operator(backend, small_test_data):

    backend.filter(Actor, {}).delete()
    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    marlon_wayans = Actor({'name': 'Marlon Wayans'})
    backend.save(marlon_brando)
    backend.save(marlon_wayans)
    backend.commit()

    assert backend.get(Actor, {'name': {'$regex': r'^Marlon\s+(?!Wayans)[\w]+$'}}) == marlon_brando
    assert len(backend.filter(Actor, {'name': {'$regex': r'^Marlon\s+.*$'}})) == 2
    assert len(backend.filter(Actor, {'name': {'$regex': r'^.*\s+Brando$'}})) == 1


def test_in(backend):
    # DB setup
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
    query = {'name': {'$in': [david_hasselhoff.name, True]}}
    assert len(backend.filter(Actor, query)) == len([david_hasselhoff])
    # Test with different types


def test_lt(backend):
    # DB setup
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

    # Test with null elements
    query = {'appearances': {'$lt': None}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with null elements


def test_gt(backend):
    # DB setup
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

    # Test with null elements
    query = {'appearances': {'$gt': None}}  # gets interpreted as 0 (zero)
    assert len(backend.filter(Actor, query)) == 4
    # Test with null elements


def test_gte(backend):
    # DB setup
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

    # Test with null elements
    query = {'appearances': {'$gte': None}}  # gets interpreted as 0 (zero)
    assert len(backend.filter(Actor, query)) == 4
    # Test with null elements


def test_lte(backend):
    # DB setup
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

    # Test with null elements
    query = {'appearances': {'$lte': None}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with null elements


def test_exists(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': 1.453, 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': 12.453, 'appearances': 34, 'is_funny': 'it depends'})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': 12.453, 'appearances': 173})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': 0.371})

    actors = [marlon_brando, leonardo_di_caprio, david_hasselhoff,
              charlie_chaplin]
    for actor in actors:
        backend.save(actor)

    backend.commit()
    assert len(backend.filter(Actor, {})) == len(actors)
    # DB setup

    # Let's count the number of documents which have particular attribute
    all_attributes = [attribute
                      for actor in actors
                      for attribute in actor.attributes.keys()
                      if attribute != 'pk']
    attributes_occurrences_counter = Counter(all_attributes)

    for attribute, count in attributes_occurrences_counter.items():
        # Test number of documents which have current attribute
        query = {attribute: {'$exists': True}}
        assert len(backend.filter(Actor, query)) == count

        # Also test number of documents which do not have current attribute
        query = {attribute: {'$exists': False}}
        assert len(backend.filter(Actor, query)) == len(actors) - count


def test_all(backend):
    # DB setup
    backend.filter(Actor, {}).delete()

    marlon_brando = Actor({'name': 'Marlon Brando', 'gross_income_m': [1.453, 1.0, 12.0], 'appearances': 78, 'is_funny': False, 'birth_year': 1924})
    leonardo_di_caprio = Actor({'name': 'Leonardo di Caprio', 'gross_income_m': [12.453, 1.0, 12.0], 'appearances': 34, 'is_funny': 'it depends', 'birth_year': 1974})
    david_hasselhoff = Actor({'name': 'David Hasselhoff', 'gross_income_m': [12.453, 1.0, 4.0], 'appearances': 173, 'is_funny': True, 'birth_year': 1952})
    charlie_chaplin = Actor({'name': 'Charlie Chaplin', 'gross_income_m': [0.371, 1.0, 99.0], 'appearances': 473, 'is_funny': True, 'birth_year': 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor, {})) == 4
    # DB setup

    # Test with normal conditions
    query = {'name': {'$all': [charlie_chaplin.name]}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test with normal conditions

    # Test with empty list
    query = {'name': {'$all': []}}
    actors = backend.filter(Actor, query)
    assert len(backend.filter(Actor, query)) == len([])
    # Test with empty list

    # Test with no result
    query = {'name': {'$all': ['jackie chan']}}
    actors = backend.filter(Actor, query)
    assert len(backend.filter(Actor, query)) == len([])
    # Test with no result

    # Test with unknown values
    query = {'name': {'$all': [jackie_chan.name]}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with unknown values

    # Test with int
    query = {'appearances': {'$all': [78]}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando])
    # Test with int

    # Test with float
    query = {'gross_income_m': {'$all': [1.0, 1.453]}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando])
    # Test with float

    # Test with full result
    query = {'gross_income_m': {'$all': [1.0]}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    # Test with full result

    # Test with boolean list
    query = {'is_funny': {'$all': [True]}}
    assert len(backend.filter(Actor, query)) == len([david_hasselhoff, charlie_chaplin])
    # Test with boolean list

    # Test with mixed values/list
    query = {'is_funny': {'$all': ['it depends', marlon_brando.name, leonardo_di_caprio.appearances, charlie_chaplin.gross_income_m]}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with mixed values/list

    # Test with crossed type
    query = {'name': {'$all': [True]}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with crossed type

    # Test with unknown attribute
    try:
        query = {'named': {'$all': [marlon_brando.name]}}
        assert len(backend.filter(Actor, query)) == len([])
    except AssertionError:
        pass
    # Test with unknwon attribute


def test_ne(backend):
    # DB setup
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
    query = {'name': {'$ne': charlie_chaplin.name}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, leonardo_di_caprio, david_hasselhoff])
    # Test with normal conditions

    # Test with empty list
    query = {'name': {'$ne': []}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin, leonardo_di_caprio, david_hasselhoff])
    # Test with empty list

    # Test with list
    query = {'name': {'$ne': [marlon_brando.name, charlie_chaplin.name]}}
    assert len(backend.filter(Actor, query)) == len([leonardo_di_caprio, david_hasselhoff, charlie_chaplin, marlon_brando])
    # Test with list

    # Test with no result
    query = {'name': {'$ne': 'jackie chan'}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, david_hasselhoff, leonardo_di_caprio, marlon_brando])
    # Test with no result

    # Test with unknown values
    query = {'name': {'$ne': jackie_chan.name}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, david_hasselhoff, leonardo_di_caprio, marlon_brando])
    # Test with unknown values

    # Test with int
    query = {'appearances': {'$ne': 78}}
    assert len(backend.filter(Actor, query)) == len([david_hasselhoff, leonardo_di_caprio, charlie_chaplin])
    # Test with int

    # Test with float/full results
    query = {'gross_income_m': {'$ne': 0.0}}
    assert len(backend.filter(Actor, query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    # Test with float/full results

    # Test with boolean
    query = {'is_funny': {'$ne': True}}
    assert len(backend.filter(Actor, query)) == len([leonardo_di_caprio, marlon_brando])
    # Test with boolean

    # Test with boolean/string
    query = {'is_funny': {'$ne': 'it depends'}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, david_hasselhoff, marlon_brando])
    # Test with boolean/string

    # Test with crossed type
    query = {'appearances': {'$ne': True}}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, leonardo_di_caprio, david_hasselhoff, marlon_brando])
    # Test with crossed type

    # Test with unknown attribute
    try:
        query = {'named': {'$ne': marlon_brando.name}}
        assert len(backend.filter(Actor, query)) == len([])
    except AssertionError:
        pass
    # Test with unknwon attribute


def test_and(backend):
    # DB setup
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
    query = {'$and': [{'name': charlie_chaplin.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test with normal conditions

    # Test with no results
    query = {'$and': [{'name': charlie_chaplin.name}, {'birth_year': 1924}, {'is_funny': 'it depends'}, {'gross_income_m': '12.453'}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with no results

    # Test repeating request
    query = {'$and': [{'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test repeating request

    # Test with no result
    query = {'$and': [{'name': charlie_chaplin.name}, {'birth_year': {'$lt': 1889}}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with no result

    # Test with no result
    query = {'$and': [{'appearances': 473}, {'birth_year': {'$lt': 1879}}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with no result

    # Test with unknown values
    query = {'$and': [{'name': jackie_chan.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with unknown values

    # Test with crossed type
    query = {'$and': [{'name': charlie_chaplin.appearances}, {'birth_year': 'may be'}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with crossed type

    # Test with unknown attribute
    query = {'$and': [{'named': charlie_chaplin.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with unknwon attribute


def test_or(backend):
    # DB setup
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
    query = {'$or': [{'name': charlie_chaplin.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test with normal conditions

    # Test with full results
    query = {'$or': [{'name': charlie_chaplin.name}, {'birth_year': 1924}, {'is_funny': 'it depends'}, {'gross_income_m': 12.453}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
    # Test with full results

    # Test repeating request
    query = {'$or': [{'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}, {'name': charlie_chaplin.name}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test repeating request

    # Test with no result
    query = {'$or': [{'name': 'Marlon not Brando'}, {'appearances': 4224}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with no result

    # Test with unknown values
    query = {'$or': [{'name': jackie_chan.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test with unknown values

    # Test with crossed type
    query = {'$or': [{'name': charlie_chaplin.appearances}, {'birth_year': 'may be'}]}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with crossed type

    # Test with unknown attribute
    query = {'$or': [{'named': charlie_chaplin.name}, {'birth_year': 1889}]}
    assert len(backend.filter(Actor, query)) == len([charlie_chaplin])
    # Test with unknwon attribute
    

def test_regex(backend):
    # DB setup
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

    # Test with unknown values
    query = {'name': {'$regex': jackie_chan.name}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with unknown values

    # Test with crossed type
    try:
        query = {'gross_income_m': {'$regex': r'^Marlon\s+.*$'}}
        assert len(backend.filter(Actor, query)) == len([])
    except TypeError:
        pass
    # Test with crossed type

    # Test with unknown attribute
    query = {'gross_income_bad': {'$regex': r'^Marlon\s+.*$'}}
    assert len(backend.filter(Actor, query)) == len([])
    # Test with unknwon attribute
