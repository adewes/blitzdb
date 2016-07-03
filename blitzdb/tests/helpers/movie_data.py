 # -*- coding: utf-8 -*-
import math
import faker  # https://github.com/joke2k/faker
import random
import uuid

from blitzdb import Document
from blitzdb.fields import (ForeignKeyField,
                            ManyToManyField,
                            CharField,
                            FloatField,
                            IntegerField,
                            BooleanField)

class Movie(Document):

    title = CharField(nullable = True,indexed = True)
    director = ForeignKeyField(related = 'Director',nullable = True,backref = 'movies')
    cast = ManyToManyField(related = 'Actor')
    year = IntegerField(indexed = True)
    best_actor = ForeignKeyField('Actor',backref = 'best_movies')

    class Meta(Document.Meta):

        dbref_includes = ['title','year']

class Actor(Document):

    name = CharField(indexed = True)
    gross_income_m = FloatField(indexed = True)
    salary_amount = FloatField(indexed = True,key = 'salary.amount')
    salary_currency = CharField(indexed = True,key = 'salary.currency')
    appearances = IntegerField(indexed = True)
    birth_year = IntegerField(indexed = True)
    favorite_food = ManyToManyField('Food')
    is_funny = BooleanField(indexed = True)
    movies = ManyToManyField('Movie',backref = 'actors')

class Food(Document):

    name = CharField(indexed = True)

class Director(Document):

    """
    Warning: There is a circular foreign key relationship between
    Director and Movie, hence trying to save a pair of those objects
    that point to each other will yield an exception for e.g.
    the Postgres backend.
    """

    name = CharField(indexed = True)
    favorite_actor = ForeignKeyField('Actor')
    best_movie = ForeignKeyField('Movie',unique=True,backref = 'best_of_director')

class Role(Document):

    role = CharField(indexed = True)
    actor = ForeignKeyField('Actor', nullable = False)
    movie = ForeignKeyField('Movie', nullable = False)

def generate_test_data(request, backend, n):

    fake = faker.Faker()

    actors = []
    movies = []
    directors = []

    backend.begin()

    backend.filter(Movie, {}).delete()
    backend.filter(Actor, {}).delete()
    backend.filter(Director, {}).delete()

    for i in range(0, n):
        movie = Movie(
            {
                'title': fake.company(),
                'year': fake.year(),
                'pk': uuid.uuid4().hex,
                'cast': [],
            }
        )
        movies.append(movie)
        movie.save(backend)

    for i in range(0, n * 4):
        actor = Actor(
            {
                'name': fake.name(),
                'pk': uuid.uuid4().hex,
                'movies' : []
            }
        )
        n_movies = 1 + int((1.0 - math.log(random.randint(1, 1000)) / math.log(1000.0)) * 5)
        actor_movies = random.sample(movies, n_movies)
        for movie in actor_movies:
            actor.movies.append(movie)
            movie.save(backend)
        actors.append(actor)
        actor.save(backend)

    for i in range(0, int(n / 10)):
        director = Director(
            {
                'name': fake.name(),
                'pk': uuid.uuid4().hex,
            }
        )
        n_movies = 2 + int((1.0 - math.log(random.randint(1, 1000)) / math.log(1000.0)) * 10)
        director_movies = random.sample(movies, n_movies)

        director.save(backend)
        for movie in director_movies:
            movie.director = director
            movie.save(backend)
        directors.append(director)
    
    backend.commit()

    return (movies, actors, directors)
