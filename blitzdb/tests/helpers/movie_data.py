import math
import faker #https://github.com/joke2k/faker
import random

from blitzdb import Document

class Movie(Document):
    pass

class Actor(Document):
    pass

class Role(Document):
    pass

class Director(Document):
    pass

def generate_test_data(request,backend,n):

    fake = faker.Faker()

    actors = []
    movies = []
    directors = []


    backend.filter(Movie,{}).delete()
    backend.filter(Actor,{}).delete()
    backend.filter(Director,{}).delete()

    for i in range(0,n):
        movie = Movie(
                {
                    'name' : fake.company(),
                    'year' : fake.year(),
                    'pk' : i,
                    'cast' : [],
                }
            )
        movies.append(movie)
        movie.save(backend)

    for i in range(0,n*4):
        actor = Actor(
            {
                'name' : fake.name(),
                'pk' : i,
                'movies' : []
            }            
            )
        n_movies = 1+int((1.0-math.log(random.randint(1,1000))/math.log(1000.0))*5)
        actor_movies = random.sample(movies,n_movies)
        for movie in actor_movies:
            actor.movies.append(movie)
            movie.cast.append({'actor':actor,'character':fake.name()})
            movie.save(backend)
        actors.append(actor)
        actor.save(backend)

    for i in range(0,int(n/10)):
        director = Director(
                {
                    'name' : fake.name(),
                    'pk' : i,
                    'movies' : [],
                }
            )
        n_movies = 1+int((1.0-math.log(random.randint(1,1000))/math.log(1000.0))*10)
        director_movies = random.sample(movies,n_movies)

        for movie in director_movies:
            movie.director = director
            movie.save(backend)
            director.movies.append(movie)
        directors.append(director)
        director.save(backend)
    
    backend.commit()

    return (movies,actors,directors)
