import pytest
import pprint

from ..helpers.movie_data import Movie,Actor,Director

from .fixtures import empty_backend
from blitzdb.backends.sql.relations import ManyToManyProxy
from blitzdb.backends.sql.queryset import QuerySet
from blitzdb.fields import ForeignKeyField
from blitzdb import Document

class User(Document):
    pass

class Subscription(Document):
    
    user = ForeignKeyField(User,unique = True,backref = 'subscription')

class Stripe(Document):

    user = ForeignKeyField(User,unique = True,backref = 'stripe')

def test_unique_backrefs(empty_backend):
    
    empty_backend.register(User)
    empty_backend.register(Subscription)
    empty_backend.register(Stripe)

    empty_backend.create_schema()

    user = User({'name' : 'test'})
    subscription = Subscription({'user' : user})
    stripe = Stripe({'user' : user})

    empty_backend.save(stripe)
    empty_backend.save(subscription)
    empty_backend.commit()

    recovered_user = empty_backend.get(User,{},include = ('subscription','stripe'))

    assert recovered_user == user
    assert not recovered_user.stripe.lazy
    assert not recovered_user.subscription.lazy
    assert isinstance(recovered_user.stripe,Stripe)
    assert isinstance(recovered_user.subscription,Subscription)
    assert recovered_user.subscription == subscription
    assert recovered_user.stripe == stripe

    assert recovered_user.stripe.user == user
    assert recovered_user.subscription.user == user

def test_non_existing_unique_backrefs(empty_backend):
    
    empty_backend.register(User)
    empty_backend.register(Subscription)
    empty_backend.register(Stripe)

    empty_backend.create_schema()

    user = User({'name' : 'test'})

    empty_backend.save(user)
    empty_backend.commit()

    recovered_user = empty_backend.get(User,{},include = ('subscription','stripe'))

    assert recovered_user == user
    assert recovered_user.stripe is None
    assert recovered_user.subscription is None
