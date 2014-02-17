#Blitz-DB

Blitzdb (or simply blitz) is a document-oriented database for Python. It can be used either as a  **stand-alone, flat-file database** or in conjunction with another database backend such as **MongoDB** or **MySQL**.

##Features

* multiple database backends (flat files, Mongo, ...)
* database transactions & operation caching
* automatic object references
* flexible querying syntax
* deep-key indexing

##Use Cases

Blitz can be used as a standalone document store for client application. Originally blitz was designed for use with the checkmate Python code analysis toolkit, where it stores statistical data. Since blitz stores all documents as single JSON files, it is possible to put the whole database under version-control.

##Examples

To get an idea of what you can do with Blitz, here are some examples.

###Creating objects

```python
from blitzdb import Object

class Movie(Object):
    pass
    
class Actor(Object):
    pass

the_godfather = Movie({'name': 'The Godfather','year':1972,'pk':1L})

marlon_brando = Actor({'name':'Marlon Brando','pk':1L})
al_pacino = Actor({'name' : 'Al Pacino','pk':1L})
```

###Storing objects in the database:

```python
from blitzdb import FileBackend

backend = FileBackend("/path/to/my/db")

the_godfather.save(backend)
marlon_brando.save(backend)
al_pacino.save(backend)
```
    
###Retrieving objects from the database:

```python
the_godfather = backend.get(Movie,{'pk':1L})
#or...
the_godfather = backend.get(Movie,{'name' : 'The Godfather'})
```
    
###Filtering objects

```python
movies_from_1972 = backend.filter(Movie,{'year' : 1972})
```

###Working with transactions

```python
backend.begin()
the_godfather.director = 'Roland Emmerich' #oops...
the_godfather.save()
backend.rollback() #undo the changes...
```

###Creating nested object references
   
```python
the_godfather.cast = {'Don Vito Corleone' : marlon_brando, 'Michael Corleone' : al_pacino}

#Objects stored within other objects will be automatically converted to database references.

marlon_brando.performances = [the_godfather]
al_pacino.performances = [the_godfather]

marlon_brando.save(backend)
al_pacino.save(backend)
the_godfather.save(backend)
#Will store references to the movies within the documents in the DB
```

###Creation of database indexes and advanced querying

```python
backend.create_index(Actor,'performances')
#Will create an index on the 'performances' field, for fast querying

godfather_cast = backend.filter(Actor,{'movies' : the_godfather})
#Will return 'Al Pacino' and 'Marlon Brando'
```

###Arbitrary filter expressions

```python
star_wars_iv = Movie({'name' : 'Star Wars - Episode IV: A New Hope','year': 1977})
star_wars_iv.save()

movies_from_the_seventies = backend.filter(Movie,{'year': lambda year : True if year >= 1970 and year < 1980 else False})
#Will return Star Wars & The Godfather (man, what a decade!)
```
