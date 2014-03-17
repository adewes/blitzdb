#Blitz-DB

**BlitzDB**, or just **Blitz** is a document-based, object-oriented, transactional database written purely in Python. Among other things, it provides a **powerful querying language**, **deep indexing of documents**, **compressed data storage** and **automatic referencing of embedded documents**. It is reasonably fast, can be easily embedded in any Python application and does not have any external dependencies (except when using a third-party backend). In addition, you can use it as a **frontend** to other database engines such as MongoDB in case you should need more power.

##[-&gt; BlitzDB Main Website & Documentation](http://blitz-db.readthedocs.org)

##Key Features

* Document-based, object-oriented interface.
* Powerful and rich querying language.
* Deep document indexes on arbitrary fields.
* Compressed storage of documents.
* Support for multiple backends (e.g. file-based storage, MongoDB).
* Support for database transactions (currently only for the file-based backend).

##Use Cases

Blitz can be used as a standalone document store for client application. Originally blitz was designed for use with the checkmate Python code analysis toolkit, where it stores statistical data. Since blitz stores all documents as single JSON files, it is possible to put the whole database under version-control.

##Installation

The easiest way to install Blitz is through **pip** or **easy_install**

    pip install blitzdb
    #or...
    easy_install blitzdb

For more detailed installation instructions, have a look at the [documentation](http://blitz-db.readthedocs.org).

##Detailed Documentation

The detailed documentation for this project is hosted on [ReadTheDocs](http://blitz-db.readthedocs.org), feel free to take a look!

##News

* 2014-03-14: Blitz is now **Python 3 compatible**, thanks to David Koblas (@koblas)!

##Examples

To get an idea of what you can do with Blitz, here are some examples.

###Creating objects

```python
from blitzdb import Document

class Movie(Document):
    pass
    
class Actor(Document):
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

#Documents stored within other objects will be automatically converted to database references.

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
