import pytest
import pprint

from ..helpers.movie_data import Movie,Actor,Director

from sqlalchemy.exc import IntegrityError

from .fixtures import backend

def test_circular_reference(backend):

    backend.init_schema()
    backend.create_schema()

    apocalypse_now = Movie({'title' : 'Apocalypse Now'})
    francis_coppola = Director({'name' : 'Francis Coppola',
                                'best_movie' : apocalypse_now})
    the_godfather = Movie({'title' : 'The Godfather',
                           'director' : francis_coppola})
    robert_de_niro = Actor({'name' : 'Robert de Niro','movies' : []})
    robert_de_niro.movies.append(the_godfather)
    francis_coppola.favorite_actor = robert_de_niro
    #this will yield an exception as we have a circular foreign key relationship
    with pytest.raises(IntegrityError):
        backend.save(robert_de_niro)
    del francis_coppola.favorite_actor
    backend.save(robert_de_niro)

def test_basics(backend):

    backend.init_schema()
    backend.create_schema()

    francis_coppola = Director({'name' : 'Francis Coppola'})
    stanley_kubrick = Director({'name' : 'Stanley Kubrick'})
    robert_de_niro = Actor({'name' : 'Robert de Niro','movies' : []})
    harrison_ford = Actor({'name' : 'Harrison Ford'})
    brian_de_palma = Director({'name' : 'Brian de Palma'})

    al_pacino = Actor({'name' : 'Al Pacino','movies' : []})

    scarface = Movie({'title' : 'Scarface','director' : brian_de_palma})

    the_godfather = Movie({'title' : 'The Godfather',
                           'director' : francis_coppola})

    space_odyssey = Movie({'title' : '2001 - A space odyssey',
                           'director' : stanley_kubrick})

    clockwork_orange = Movie({'title' : 'A Clockwork Orange',
                              'director' : stanley_kubrick})

    robert_de_niro.movies.append(the_godfather)
    al_pacino.movies.append(the_godfather)
    al_pacino.movies.append(scarface)
    stanley_kubrick.favorite_actor = al_pacino

    apocalypse_now = Movie({'title' : 'Apocalypse Now'})
    star_wars_v = Movie({'title' : 'Star Wars V: The Empire Strikes Back'})
    harrison_ford.movies = [star_wars_v]

    backend.save(robert_de_niro)
    backend.save(al_pacino)

    backend.update(francis_coppola,{'best_movie' : the_godfather})

    francis_coppola.favorite_actor = robert_de_niro

    backend.save(francis_coppola)
    backend.save(stanley_kubrick)
    backend.save(brian_de_palma)
    backend.save(harrison_ford)

    backend.save(the_godfather)
    backend.save(clockwork_orange)
    backend.save(space_odyssey)
    backend.save(scarface)

    backend.commit()

    #we have a backreference from director to movies

    assert backend.get(Director,{'movies' : {'$all' : [the_godfather]}}) == francis_coppola
    assert backend.get(Director,{'movies.title' : {'$in' : ['Apocalypse Now','The Godfather']}}) == francis_coppola

    result = backend.filter(Movie,{'$or' : [{'actors' : harrison_ford},
                                            {'actors.name' : 'Al Pacino'},
                                            {'actors.name' : 'Robert de Niro'}]})

    assert len(result) == 3
    assert scarface in result
    assert the_godfather in result
    assert star_wars_v in result

    result = backend.filter(Movie,{'actors' : {'$all': [al_pacino,robert_de_niro]}})
    assert len(result) == 1
    assert result[0] == the_godfather

    result = backend.filter(Director,{'movies.title' : 'The Godfather'}) 

    al_pacino_movies = backend.filter(Movie,{'actors' : al_pacino})
    assert len(al_pacino_movies) == 2
    assert the_godfather in al_pacino_movies
    assert scarface in al_pacino_movies

    assert len(result) == 1
    assert francis_coppola in result

    result = backend.filter(Movie,{'actors' : {'$all' : [stanley_kubrick.favorite_actor,
                                                         francis_coppola.favorite_actor]}})

    assert len(result) == 1
    assert result[0] == the_godfather

    result = backend.filter(Director,{'movies.actors' : al_pacino})
