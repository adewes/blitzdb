import math
import faker  # https://github.com/joke2k/faker
import random

from blitzdb import Document

try:
    #we try to import String types from SQLAlchemy
    from sqlalchemy.types import String,Float,Integer,Boolean
except ImportError:
    pass

class Movie(Document):
    
    class Meta(Document.Meta):

        dbref_includes = ['title']

        indexes = [
            {
                'sql' : lambda: {
                    'field' : 'tags',
                    'list' : True,
                    'type' : String,
                }
            },
            {
                'sql' : lambda: {
                    'field' : 'title',
                    'type' : String,
                }
            }
        ]

        relations = [
            {
                'field' : 'director',
                'type' : 'ForeignKey',
                'related' : 'Actor',
                'nullable' : True,
            },
        ]

class Actor(Document):
    
    class Meta(Document.Meta):

        indexes = [
            {
                'sql' : lambda: {
                    'field' : 'name',
                    'type' : String,
                }
            },
            {
                'sql' : lambda: {
                    'field' : 'gross_income_m',
                    'type' : Float,
                    'nullable' : True,
                }
            },
            {
                'sql' : lambda: {
                    'field' : 'appearances',
                    'type' : Integer,
                    'nullable' : True,
                }
            },
            {
                'sql' : lambda: {
                    'field' : 'favorite_food',
                    'type' : String,
                    'list' : True
                }
            },
            {
                'sql' : lambda: {
                    'field' : 'is_funny',
                    'type' : Boolean,
                    'nullable' : True,
                }
            },
        ]

        """
        Relations to other tables
        """

        relations = [
            {
                'field' : 'movies',
                'type' : 'ManyToMany',
                'related' : 'Movie',
#                'qualifier' : 'role'
            },
        ]

class Director(Document):
    
    class Meta(Document.Meta):

        indexes = [
            {
                'sql' : lambda: {
                    'field' : 'name',
                    'type' : String,
                }
            }
        ]

class Role(Document):

    class Meta(Document.Meta):

        indexes = [
            {
                'sql' : lambda: {
                    'field' : 'role',
                    'type' : String,
                }
            },
        ]

        """
        Relations to other tables
        """

        relations = [
            {
                'field' : 'actor',
                'type' : 'ForeignKey',
                'related' : 'Actor',
                'nullable' : False,
            },
            {
                'field' : 'movie',
                'type' : 'ForeignKey',
                'related' : 'Movie',
                'nullable' : False,
            },
        ]


def generate_test_data(request, backend, n):

    fake = faker.Faker()

    actors = []
    movies = []
    directors = []

    backend.filter(Movie, {}).delete()
    backend.filter(Actor, {}).delete()
    backend.filter(Director, {}).delete()

    for i in range(0, n):
        movie = Movie(
            {
                'title': fake.company(),
                'year': fake.year(),
                'pk': i,
                'cast': [],
            }
        )
        movies.append(movie)
        movie.save(backend)

    for i in range(0, n * 4):
        actor = Actor(
            {
                'name': fake.name(),
                'pk': i,
                'movies': []
            }            
        )
        n_movies = 1 + int((1.0 - math.log(random.randint(1, 1000)) / math.log(1000.0)) * 5)
        actor_movies = random.sample(movies, n_movies)
        for movie in actor_movies:
            actor.movies.append(movie)
            movie.cast.append({'actor': actor, 'character': fake.name()})
            movie.save(backend)
        actors.append(actor)
        actor.save(backend)

    for i in range(0, int(n / 10)):
        director = Director(
            {
                'name': fake.name(),
                'pk': i,
                'movies': [],
            }
        )
        n_movies = 1 + int((1.0 - math.log(random.randint(1, 1000)) / math.log(1000.0)) * 10)
        director_movies = random.sample(movies, n_movies)

        for movie in director_movies:
            movie.director = director
            movie.save(backend)
            director.movies.append(movie)
        directors.append(director)
        director.save(backend)
    
    backend.commit()

    return (movies, actors, directors)
