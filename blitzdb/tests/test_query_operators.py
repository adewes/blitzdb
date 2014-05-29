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
