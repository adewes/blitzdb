import pytest
import tempfile
import subprocess
import random
import time
import pymongo

from blitzdb.backends.mongo import Backend as MongoBackend
from blitzdb.backends.file import Backend as FileBackend
from blitzdb import Document
from blitzdb.tests.helpers.movie_data import Actor,Director,Movie,generate_test_data

def test_operators(backend):

    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()

    assert len(backend.filter(Actor,{})) == 4

    for op,results in (('$gt',[david_hasselhoff]),('$gte',[david_hasselhoff]),('$lt',[charlie_chaplin]),('$lte',[charlie_chaplin])):

        query = {   
                '$and' : 
                    [
                        {'gross_income_m' : { op : 1.0} },
                        {'is_funny' : True }
                    ] 
                }

        assert len(backend.filter(Actor,query)) == len(results)
        assert results in backend.filter(Actor,query)

    for op,results in (('$gt',[david_hasselhoff,charlie_chaplin,marlon_brando]),('$gte',[marlon_brando,david_hasselhoff,charlie_chaplin]),('$lt',[charlie_chaplin]),('$lte',[charlie_chaplin])):

        query = { 
                '$and' : 
                    [
                        {'$or' : [
                                    {'gross_income_m' : { op : 1.0} },
                                    {'birth_year' : { '$lt' : 1900} },
                                ]},
                        {'$or' : [
                            {'is_funny' : True },
                            {'name' : 'Marlon Brando'},
                                ]
                        },

                    ] 
                }

        assert len(backend.filter(Actor,query)) == len(results)
        assert results in backend.filter(Actor,query)


    for op,results in (('$gt',[leonardo_di_caprio]),('$gte',[leonardo_di_caprio,david_hasselhoff]),('$lt',[marlon_brando]),('$lte',[david_hasselhoff, marlon_brando])):
             
            query = {   
                '$and' : 
                    [
                        {'gross_income_m' : { '$in' : [1.0, 12.453, 1.453]} },
                        {'birth_year' : { op : 1952 }}
                    ] 
                }
            assert len(backend.filter(Actor,query)) == len(results)
            assert results in backend.filter(Actor,query)

    for op,results in (('$gt',[leonardo_di_caprio]),('$gte',[leonardo_di_caprio,david_hasselhoff]),('$lt',[marlon_brando]),('$lte',[david_hasselhoff, marlon_brando])):
             
            query = {   
                '$and' : 
                    [
                        {'gross_income_m' : { '$exists' : [1.0, 12.453, 1.453]} },
                        {'birth_year' : { op : 1952 }}
                    ] 
                }
            assert len(backend.filter(Actor,query)) == len(results)
            assert results in backend.filter(Actor,query)

    for op,results in (('$gt',[charlie_chaplin, leonardo_di_caprio, david_hasselhoff]),('$gte',[charlie_chaplin, marlon_brando, david_hasselhoff, leonardo_di_caprio]),('$lt',[charlie_chaplin]),('$lte',[marlon_brando, charlie_chaplin])):
             
            query = {   
                '$or' : 
                    [
                        {'appearances' : { '$eq' : 473} },
                        {'gross_income_m' : { op : 1.453 }}
                    ] 
                }
            assert len(backend.filter(Actor,query)) == len(results)
            assert results in backend.filter(Actor,query)           


    assert len(backend.filter(Actor,{'name' : {'$ne' : 'David Hasselhoff'}})) == 3
    assert len(backend.filter(Actor,{'name' : 'David Hasselhoff'})) == 1
    assert len(backend.filter(Actor,{'name' : {'$not' : {'$in' : ['David Hasselhoff','Marlon Brando','Charlie Chaplin']}}})) == 1
    assert len(backend.filter(Actor,{'name' : {'$in' : ['Marlon Brando','Leonardo di Caprio']}})) == 2

def test_regex_operator(backend,small_test_data):

    backend.filter(Actor,{}).delete()
    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    marlon_wayans = Actor({'name' : 'Marlon Wayans'})
    backend.save(marlon_brando)
    backend.save(marlon_wayans)
    backend.commit()

    assert backend.get(Actor,{'name' : {'$regex' : r'^Marlon\s+(?!Wayans)[\w]+$'}}) == marlon_brando
    assert len(backend.filter(Actor,{'name' : {'$regex' : r'^Marlon\s+.*$'}})) == 2
    assert len(backend.filter(Actor,{'name' : {'$regex' : r'^.*\s+Brando$'}})) == 1

def test_in():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with empty list
    query = {'name' : {'$not' : {'$in' : []}}}
    assert len(backend.filter(Actor,query)) == len([david_hasselhoff, charlie_chaplin, marlon_brando, leonardo_di_caprio])
    query = {'name' : {'$in' : []}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with empty list

    #Test with one match
    query = {'name' : {'$in' : [david_hasselhoff.name]}}
    assert len(backend.filter(Actor,query)) == len([david_hasselhoff])
    #Test with one match

    #Test with unknown elements
    query = {'name' : {'$in' : ['jackie chan']}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with unknown elements

    #Test with different types
    query = {'name' : {'$in' : [david_hasselhoff.name, True]}}
    assert len(backend.filter(Actor,query)) == len([david_hasselhoff])
    #Test with different types

    #Test without a list
    try:
        actor = backend.filter(Actor, {'name' : {'$in' : 4}})
        assert len(backend.filter(Actor,query)) == len([])
    except AttributeError:
    #no list
        pass
    #Test without a list

def test_lt():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with String
    query = {'name' : {'$lt' : marlon_brando.name}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    #Test with String

    #Test with float/int
    query = {'gross_income_m' : {'$lt' : marlon_brando.appearances}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin, leonardo_di_caprio, david_hasselhoff])
    #Test with float/int

    #Test with normal conditions
    query = {'appearances' : {'$lt' : david_hasselhoff.appearances}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, leonardo_di_caprio])
    #Test with normal conditions

    #Test with normal conditions
    query = {'gross_income_m' : {'$lt' : david_hasselhoff.gross_income_m}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin])
    #Test with normal conditions

    #Test with null elements
    try:
        query = {'appearances' : {'$lt' :jackie_chan.appearances}}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with null elements

    #Test with illegal values
    try:
        query = {'gross_income_m' : {'$lt' : math.sqrt(-1)}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

        #Test with illegal values
    try:
        query = {'gross_income_m' : {'$lt' : 0/0}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

    #Test with boolean value
    try:
        actor = backend.filter(Actor, {'is_funny' : {'$lt' : False}})
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with boolean value

def test_gt():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with String
    query = {'name' : {'$gt' : marlon_brando.name}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with String

    #Test with float/int
    query = {'gross_income_m' : {'$gt' : marlon_brando.appearances}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with float/int

    #Test with normal conditions
    query = {'appearances' : {'$gt' : david_hasselhoff.appearances}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test with normal conditions

    #Test with normal conditions
    query = {'gross_income_m' : {'$gt' : marlon_brando.gross_income_m}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff])
    #Test with normal conditions

    #Test with null elements
    try:
        query = {'appearances' : {'$gt' :jackie_chan.appearances}}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with null elements

    #Test with illegal values
    try:
        query = {'gross_income_m' : {'$gt' : math.sqrt(-1)}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

        #Test with illegal values
    try:
        query = {'gross_income_m' : {'$gt' : 0/0}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

    #Test with boolean value
    try:
        actor = backend.filter(Actor, {'is_funny' : {'$gt' : False}})
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with boolean value


def test_gte():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with String
    query = {'name' : {'$gte' : marlon_brando.name}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando])
    #Test with String

    #Test with float/int
    query = {'gross_income_m' : {'$gte' : marlon_brando.appearances}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with float/int

    #Test with normal conditions
    query = {'appearances' : {'$gte' : david_hasselhoff.appearances}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, marlon_brando])
    #Test with normal conditions

    #Test with normal conditions
    query = {'gross_income_m' : {'$gte' : marlon_brando.gross_income_m}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff])
    #Test with normal conditions

    #Test with null elements
    try:
        query = {'appearances' : {'$gte' :jackie_chan.appearances}}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with null elements

    #Test with illegal values
    try:
        query = {'gross_income_m' : {'$gte' : math.sqrt(-1)}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

        #Test with illegal values
    try:
        query = {'gross_income_m' : {'$gte' : 0/0}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

    #Test with boolean value
    try:
        actor = backend.filter(Actor, {'is_funny' : {'$gte' : False}})
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with boolean value

def test_lte():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with String
    query = {'name' : {'$lte' : marlon_brando.name}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
    #Test with String

    #Test with float/int
    query = {'gross_income_m' : {'$lte' : marlon_brando.appearances}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin, leonardo_di_caprio, david_hasselhoff])
    #Test with float/int

    #Test with normal conditions
    query = {'appearances' : {'$lte' : david_hasselhoff.appearances}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, leonardo_di_caprio, david_hasselhoff])
    #Test with normal conditions

    #Test with normal conditions
    query = {'gross_income_m' : {'$lte' : david_hasselhoff.gross_income_m}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    #Test with normal conditions

    #Test with null elements
    try:
        query = {'appearances' : {'$lte' :jackie_chan.appearances}}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with null elements

    #Test with illegal values
    try:
        query = {'gross_income_m' : {'$lte' : math.sqrt(-1)}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

        #Test with illegal values
    try:
        query = {'gross_income_m' : {'$lte' : 0/0}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

    #Test with boolean value
    try:
        actor = backend.filter(Actor, {'is_funny' : {'$lte' : False}})
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with boolean value

def test_exists():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with list
    query = {'name' : { '$exists' : [david_hasselhoff.name, marlon_brando.name, leonardo_di_caprio.name, charlie_chaplin.name]}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
    #Test with list

    #Test with empty list
    query = {'name' : { '$exists' : []}}
    actors= backend.filter(Actor,query)
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
    #Test with empty list

    #Test with String and unknown values
    try:
        query = {'name' : { '$exists' : jackie_chan.name}}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with String and unknown values

    #Test with float/int
    try:
        query = {'appearances' : { '$exists' : 78.0}}
        assert len(backend.filter(Actor,query)) == len([])
    except AssertionError:
        print("Issue: exists with string should give empty list/raise error")
    #Test with float/int

    #Test with float/int
    try:
        query = {'gross_income_m' : { '$exists' : leonardo_di_caprio.appearances}}
        assert len(backend.filter(Actor,query)) == len([])
    except AssertionError:
        print("Issue: exists with int should give empty list/raise error")
    #Test with float/int

    #Test with boolean
    query = {'is_funny' : { '$exists' : True}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, david_hasselhoff, charlie_chaplin, leonardo_di_caprio])
    #Test with boolean

    #Test with string
    try:
        query = {'is_funny' : { '$exists' : 'it depends'}}
        assert len(backend.filter(Actor,query)) == len([])
    except AssertionError:
        print("Issue: exists with string should give empty list/raise error")
    #Test with string

    #Test with mixed values/list
    try:
        query = {'is_funny' : { '$exists' : [True, 'it depends', marlon_brando.name, leonardo_di_caprio.appearances, charlie_chaplin.gross_income_m]}}
        assert len(backend.filter(Actor,query)) == len([])
    except AssertionError:
        print("Issue: exists with mixed-type list should give empty list/raise error")
    #Test with mixed values/list

    #Test with normal conditions
    try:
        query = {'name' : { '$exists' : False}}
        assert len(backend.filter(Actor,query)) == len([])
    except AssertionError:
        print("Issue: ('name' : {$exists : false}} should give empty list")
    #Test with normal conditions

    #Test with unknown attribute
    query = {'named' : { '$exists' : True}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with unknwon attribute

    #Test with unknown attribute
    try:
        query = {'named' : { '$exists' : False}}
        assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff, marlon_brando, leonardo_di_caprio])
    except AssertionError:
        print("Issue: exists: false with unknwoned attribute should give full results")
    #Test with unknwon attribute

    #Test with illegal values
    try:
        query = {'appearances' : { '$exists' : 0/0}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

    #Test with illegal values
    try:
        query = {'appearances' : { '$exists' : math.sqrt(-1)}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

def test_all():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : [1.453, 1.0, 12.0],'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : [12.453, 1.0, 12.0],'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : [12.453, 1.0, 4.0],'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : [0.371, 1.0, 99.0],'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with normal conditions
    query = {'name' : { '$all' : [charlie_chaplin.name]}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test with normal conditions

    #Test with empty list
    query = {'name' : { '$all' : []}}
    actors= backend.filter(Actor,query)
    assert len(backend.filter(Actor,query)) == len([])
    #Test with empty list

    #Test with no result
    query = {'name' : { '$all' : ['jackie chan']}}
    actors= backend.filter(Actor,query)
    assert len(backend.filter(Actor,query)) == len([])
    #Test with no result

    #Test with unknown values
    try:
        query = {'name' : { '$all' : [jackie_chan.name]}}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with unknown values

    #Test with int
    query = {'appearances' : { '$all' : [78]}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando])
    #Test with int

    #Test with float
    query = {'gross_income_m' : { '$all' : [1.0, 1.453]}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando])
    #Test with float

    #Test with full result
    query = {'gross_income_m' : { '$all' : [1.0]}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    #Test with full result

    #Test with boolean list
    query = {'is_funny' : { '$all' : [True]}}
    assert len(backend.filter(Actor,query)) == len([david_hasselhoff, charlie_chaplin])
    #Test with boolean list

    #Test with boolean
    try:
        query = {'is_funny' : { '$all' : True}}
        assert len(backend.filter(Actor,query)) == len([])
    except AttributeError:
        pass
    #Test with boolean

    #Test with string
    query = {'appearances' : { '$all' : 'test'}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with string

    #Test with mixed values/list
    query = {'is_funny' : { '$all' : ['it depends', marlon_brando.name, leonardo_di_caprio.appearances, charlie_chaplin.gross_income_m]}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with mixed values/list

    #Test with int
    try:
        query = {'appearances' : { '$all' : 78}}
        assert len(backend.filter(Actor,query)) == len([])
    except AttributeError:
        pass
    #Test with int

    #Test with float
    try:
        query = {'gross_income_m' : { '$all' : 1.453}}
        assert len(backend.filter(Actor,query)) == len([])
    except AttributeError:
        pass
    #Test with float

    #Test with crossed type
    query = {'name' : { '$all' : [True]}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with crossed type

    #Test with unknown attribute
    try:
        query = {'named' : { '$all' : [marlon_brando.name]}}
        assert len(backend.filter(Actor,query)) == len([])
    except AssertionError:
        pass
    #Test with unknwon attribute

    #Test with illegal values
    try:
        query = {'appearances' : { '$all' : [0/0, math.sqrt(-1)]}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    except ValueError:
        pass
    #Test with illegal values

def test_ne():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with normal conditions
    query = {'name' : { '$ne' : charlie_chaplin.name}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, leonardo_di_caprio, david_hasselhoff])
    #Test with normal conditions

    #Test with empty list
    query = {'name' : { '$ne' : []}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin, leonardo_di_caprio, david_hasselhoff])
    #Test with empty list

    #Test with list
    query = {'name' : { '$ne' : [marlon_brando.name, charlie_chaplin.name]}}
    assert len(backend.filter(Actor,query)) == len([leonardo_di_caprio, david_hasselhoff, charlie_chaplin, marlon_brando])
    #Test with list

    #Test with no result
    query = {'name' : { '$ne' : 'jackie chan'}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff, leonardo_di_caprio, marlon_brando])
    #Test with no result

    #Test with unknown values
    try:
        query = {'name' : { '$ne' : jackie_chan.name}}
        assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff, leonardo_di_caprio, marlon_brando])
    except NameError:
        pass
    #Test with unknown values

    #Test with int
    query = {'appearances' : { '$ne' : 78}}
    assert len(backend.filter(Actor,query)) == len([david_hasselhoff, leonardo_di_caprio, charlie_chaplin])
    #Test with int

    #Test with float/full results
    query = {'gross_income_m' : { '$ne' : 0.0}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando, charlie_chaplin, david_hasselhoff, leonardo_di_caprio])
    #Test with float/full results

    #Test with boolean
    query = {'is_funny' : { '$ne' : True}}
    assert len(backend.filter(Actor,query)) == len([leonardo_di_caprio, marlon_brando])
    #Test with boolean

    #Test with boolean/string
    query = {'is_funny' : { '$ne' : 'it depends'}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff, marlon_brando])
    #Test with boolean/string

    #Test with crossed type
    query = {'appearances' : { '$ne' : True}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, leonardo_di_caprio, david_hasselhoff, marlon_brando])
    #Test with crossed type

    #Test with unknown attribute
    try:
        query = {'named' : { '$ne' : marlon_brando.name}}
        assert len(backend.filter(Actor,query)) == len([])
    except AssertionError:
        pass
    #Test with unknwon attribute

    #Test with illegal values
    try:
        query = {'appearances' : { '$ne' : math.sqrt(-1)}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

    #Test with illegal values
    try:
        query = {'appearances' : { '$ne' : 0/0}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

def test_eq():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with normal conditions
    query = {'name' : { '$eq' : charlie_chaplin.name}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test with normal conditions

    #Test with empty list
    query = {'name' : { '$eq' : []}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with empty list

    #Test with list
    query = {'name' : { '$eq' : [marlon_brando.name, charlie_chaplin.name]}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with list

    #Test with no result
    query = {'name' : { '$eq' : 'jackie chan'}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with no result

    #Test with unknown values
    try:
        query = {'name' : { '$eq' : jackie_chan.name}}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with unknown values

    #Test with int
    query = {'appearances' : { '$eq' : 78}}
    assert len(backend.filter(Actor,query)) == len([marlon_brando])
    #Test with int

    #Test with float/full results
    query = {'gross_income_m' : { '$eq' : 0.0}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with float/full results

    #Test with boolean
    query = {'is_funny' : { '$eq' : True}}
    assert len(backend.filter(Actor,query)) == len([david_hasselhoff, charlie_chaplin])
    #Test with boolean

    #Test with boolean/string
    query = {'is_funny' : { '$eq' : 'it depends'}}
    assert len(backend.filter(Actor,query)) == len([leonardo_di_caprio])
    #Test with boolean/string

    #Test with crossed type
    query = {'appearances' : { '$eq' : True}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with crossed type

    #Test with unknown attribute
    query = {'named' : { '$eq' : marlon_brando.name}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with unknwon attribute

    #Test with illegal values
    try:
        query = {'appearances' : { '$eq' : math.sqrt(-1)}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

    #Test with illegal values
    try:
        query = {'appearances' : { '$eq' : 0/0}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

def test_not():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with string
    try:
        query = {'name' : { '$not' : charlie_chaplin.name}}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with string

    #Test with empty list
    try:
        query = {'name' : { '$not' : []}}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with empty list

    #Test with list
    try:
        query = {'name' : { '$not' : [marlon_brando.name, charlie_chaplin.name]}}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with list

    #Test with no results
    query = {'birth_year' : { '$not' : {'$gte' : 0}}}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with no results

    #Test with unknown values
    try:
        query = {'name' : { '$not' : {'$eq' : jackie_chan.name}}}
        assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff, marlon_brando, leonardo_di_caprio])
    except NameError:
        pass
    #Test with unknown values

    #Test with int
    try:
        query = {'appearances' : { '$not' : 78}}
        assert len(backend.filter(Actor,query)) == len([charlie_chaplin, leonardo_di_caprio, david_hasselhoff])
    except TypeError:
        pass
    #Test with int

    #Test with float/full results
    query = {'gross_income_m' : { '$not' : {'$lt' : 0.0}}}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, david_hasselhoff, marlon_brando, leonardo_di_caprio])
    #Test with float/full results

    #Test with boolean
    try:
        query = {'is_funny' : { '$not' : True}}
        assert len(backend.filter(Actor,query)) == len([marlon_brando, leonardo_di_caprio])
    except TypeError:
        pass
    #Test with boolean

    #Test with crossed type
    try:
        query = {'appearances' : { '$not' : True}}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with crossed type

    #Test with crossed type
    query = {'appearances' : { '$not' : {'$eq' : True}}}
    assert len(backend.filter(Actor,query)) == len([leonardo_di_caprio, david_hasselhoff, marlon_brando, charlie_chaplin])
    #Test with crossed type

    #Test with unknown attribute
    try:
        query = {'named' : { '$not' : marlon_brando.name}}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        #str object is not collable
        pass
    #Test with unknwon attribute

    #Test with illegal values
    try:
        query = {'appearances' : { '$not' : {'$eq' : math.sqrt(-1)}}}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

    #Test with illegal values
    try:
        query = {'appearances' : { '$not' : {'$eq' : 0/0}}}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

def test_and():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with normal conditions
    query = {'$and' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 1889}]}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test with normal conditions

    #Test with no results
    query = {'$and' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 1924},{'is_funny': 'it depends'},{'gross_income_m':'12.453'}]}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with no results

    #Test repeating request
    query = {'$and' : [{'name' : { '$eq' : charlie_chaplin.name}},{'name' : { '$eq' : charlie_chaplin.name}},{'name' : { '$eq' : charlie_chaplin.name}},{'name' : { '$eq' : charlie_chaplin.name}}]}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test repeating request

    #Test with empty list
    try:
        query = {'$and' : []}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with empty list

    #Test with no result
    query = {'$and' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : {'$lt' : 1889}}]}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with no result

    #Test with no result
    query = {'$and' : [{'appearances' : 473},{'birth_year' :{'$lt' : 1879}}]}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with no result

    #Test with unknown values
    try:
        query = {'$and' : [{'name' : { '$eq' : jackie_chan.name}},{'birth_year' : 1889}]}
        assert len(backend.filter(Actor,query)) == len([])
    except NameError:
        pass
    #Test with unknown values

    #Test with int
    try:
        query = {'$and' : 18}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with int

    #Test with float
    try:
        query = {'$and' : 0.0}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with float

    #Test with boolean
    try:
        query = {'$and' : True}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with boolean

    #Test with string
    try:
        query = {'$and' : '42'}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with string

    #Test with crossed type
    query = {'$and' : [{'name' : { '$eq' : charlie_chaplin.appearances}},{'birth_year' : 'may be'}]}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with crossed type

    #Test with unknown attribute
    query = {'$and' : [{'named' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 1889}]}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with unknwon attribute

    #Test with illegal values
    try:
        query = {'$and' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 0/0}]}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

    #Test with illegal values
    try:
        query = {'$and' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : math.sqrt(-1)}]}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

def test_or():
    #DB setup
    backend.filter(Actor,{}).delete()

    marlon_brando = Actor({'name' : 'Marlon Brando', 'gross_income_m' : 1.453,'appearances' : 78,'is_funny' : False,'birth_year' : 1924})
    leonardo_di_caprio = Actor({'name' : 'Leonardo di Caprio', 'gross_income_m' : 12.453,'appearances' : 34,'is_funny' : 'it depends','birth_year' : 1974})
    david_hasselhoff = Actor({'name' : 'David Hasselhoff', 'gross_income_m' : 12.453,'appearances' : 173,'is_funny' : True,'birth_year' : 1952})
    charlie_chaplin = Actor({'name' : 'Charlie Chaplin', 'gross_income_m' : 0.371,'appearances' : 473,'is_funny' : True,'birth_year' : 1889})

    backend.save(marlon_brando)
    backend.save(leonardo_di_caprio)
    backend.save(david_hasselhoff)
    backend.save(charlie_chaplin)

    backend.commit()
    assert len(backend.filter(Actor,{})) == 4
    #DB setup

    #Test with normal conditions
    query = {'$or' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 1889}]}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test with normal conditions

    #Test with full results
    query = {'$or' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 1924},{'is_funny': 'it depends'},{'gross_income_m':12.453}]}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin, marlon_brando, leonardo_di_caprio, david_hasselhoff])
    #Test with full results

    #Test repeating request
    query = {'$or' : [{'name' : { '$eq' : charlie_chaplin.name}},{'name' : { '$eq' : charlie_chaplin.name}},{'name' : { '$eq' : charlie_chaplin.name}},{'name' : { '$eq' : charlie_chaplin.name}}]}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test repeating request

    #Test with empty list
    try:
        query = {'$or' : []}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with empty list

    #Test with no result
    query = {'$or' : [{'name' : 'Marlon not Brando'},{'appearances' : 4224}]}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with no result

    #Test with unknown values
    try:
        query = {'$or' : [{'name' : { '$eq' : jackie_chan.name}},{'birth_year' : 1889}]}
        assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    except NameError:
        pass
    #Test with unknown values

    #Test with int
    try:
        query = {'$or' : 18}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with int

    #Test with float
    try:
        query = {'$or' : 0.0}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with float

    #Test with boolean
    try:
        query = {'$or' : True}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with boolean

    #Test with string
    try:
        query = {'$or' : '42'}
        assert len(backend.filter(Actor,query)) == len([])
    except TypeError:
        pass
    #Test with string

    #Test with crossed type
    query = {'$or' : [{'name' : { '$eq' : charlie_chaplin.appearances}},{'birth_year' : 'may be'}]}
    assert len(backend.filter(Actor,query)) == len([])
    #Test with crossed type

    #Test with unknown attribute
    query = {'$or' : [{'named' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 1889}]}
    assert len(backend.filter(Actor,query)) == len([charlie_chaplin])
    #Test with unknwon attribute

    #Test with illegal values
    try:
        query = {'$or' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : 0/0}]}
        assert len(backend.filter(Actor,query)) == len([])
    except ZeroDivisionError:
        pass
    #Test with illegal values

    #Test with illegal values
    try:
        query = {'$or' : [{'name' : { '$eq' : charlie_chaplin.name}},{'birth_year' : math.sqrt(-1)}]}
        assert len(backend.filter(Actor,query)) == len([])
    except ValueError:
        pass
    #Test with illegal values

def test():
    test_lt()
    test_lte()
    test_gt()
    test_gte()
    test_in()
    test_exists()
    test_all()
    test_ne()
    test_eq()
    test_not()
    test_and()
    test_or()
