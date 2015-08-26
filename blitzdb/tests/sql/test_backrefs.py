import pytest
import pprint

from ..helpers.movie_data import Movie,Actor,Director

from .fixtures import backend

def test_basics(backend):

    backend.init_schema()
    backend.create_schema()

    francis_coppola = Director({'name' : 'Francis Coppola'})
    robert_de_niro = Actor({'name' : 'Robert de Niro','movies' : []})
    al_pacino = Actor({'name' : 'Al Pacino','movies' : []})

    the_godfather = Movie({'title' : 'The Godfather',
                           'director' : francis_coppola})

    robert_de_niro.movies.append(the_godfather)
    al_pacino.movies.append(the_godfather)

    apocalypse_now = Movie({'title' : 'Apocalypse Now'})
    star_wars_v = Movie({'title' : 'Star Wars V: The Empire Strikes Back'})

    backend.save(the_godfather)
    backend.save(robert_de_niro)
    backend.save(al_pacino)

    #we have a backreference from director to movies

    assert backend.get(Director,{'related_movie' : {'$all' : [the_godfather]}}) == francis_coppola
    assert backend.get(Movie,{'actors' : {'$all' : []}}) == the_godfather