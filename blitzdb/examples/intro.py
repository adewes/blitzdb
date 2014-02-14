from blitzdb import Object

class Movie(Object):
    pass

class Actor(Object):
    pass

the_godfather = Movie({'name': 'The Godfather','year':1972,'pk':1L})

marlon_brando = Actor({'name':'Marlon Brando','pk' : 1L})
al_pacino = Actor({'name' : 'Al Pacino','pk' : 2L})

from blitzdb import FileBackend

backend = FileBackend("/tmp/movies")

backend.register(Movie,{'collection':'movies'})
backend.register(Actor,{'collection':'actors'})

backend.filter(Movie,{}).delete()
backend.filter(Actor,{}).delete()

the_godfather.save(backend)
marlon_brando.save(backend)
al_pacino.save(backend)

the_godfather = backend.get(Movie,{'pk':1L})
#or...
the_godfather = backend.get(Movie,{'name' : 'The Godfather'})

print the_godfather

movies_from_1972 = backend.filter(Movie,{'year' : 1972})

the_godfather.cast = {'Don Vito Corleone' : marlon_brando, 'Michael Corleone' : al_pacino}

#Objects stored within other objects will be automatically converted to database references.

marlon_brando.performances = [the_godfather]
al_pacino.performances = [the_godfather]

marlon_brando.save(backend)
al_pacino.save(backend)

backend.create_index(Actor,'performances')
#Will create an index for fast querying


godfather_cast = backend.filter(Actor,{'performances' : the_godfather})
#Will return 'Al Pacino' and 'Marlon Brando'

print "Godfather cast:",list(godfather_cast)

star_wars_iv = Movie({'name' : 'Star Wars - Episode IV: A New Hope','year': 1977})
star_wars_iv.save(backend)
movies_from_the_seventies = backend.filter(Movie,{'year': lambda year : True if year >= 1970 and year < 1980 else False})

print "Movies from the seventies:",list(movies_from_the_seventies)