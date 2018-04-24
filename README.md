# Blitz-DB

[![Build Status](https://travis-ci.org/adewes/blitzdb.svg?branch=master)](https://travis-ci.org/adewes/blitzdb)
[![PyPI](https://img.shields.io/pypi/v/blitzdb.svg?maxAge=1000)](https://pypi.python.org/pypi/blitzdb)
[![Code Issues](http://www.quantifiedcode.com/api/v1/project/gh:adewes:blitzdb/badge.svg)](http://www.quantifiedcode.com/app/project/gh:adewes:blitzdb)
[![Python 3](http://img.shields.io/badge/Python%203%20-compatible-brightgreen.svg)](https://www.python.org/download/releases/3.0/)

**BlitzDB**, or just **Blitz** is a document-based, object-oriented, transactional database written purely in Python. Among other things, it provides a **powerful querying language**, **deep indexing of documents**, **compressed data storage** and **automatic referencing of embedded documents**. It is reasonably fast, can be easily embedded in any Python application and does not have any external dependencies (except when using a third-party backend). In addition, you can use it as a **frontend** to other database engines such as MongoDB in case you should need more power.

## [Go To Main Documentation](http://blitzdb.readthedocs.org)

## Key Features

* Document-based, object-oriented interface.
* Powerful and rich querying language.
* Deep document indexes on arbitrary fields.
* Compressed storage of documents.
* Support for multiple backends (e.g. file-based storage, MongoDB).
* Support for database transactions (currently only for the file-based backend).

## Use Cases

Blitz can be used as a standalone document store for client application. Originally blitz was designed for use with the [checkmate](https://github.com/quantifiedcode/checkmate) Python code analysis toolkit, where it stores statistical data. Since blitz stores all documents as single JSON files, it is possible to put the whole database under version-control.

## Installation

The easiest way to install Blitz is through **pip** or **easy_install**

    pip install blitzdb
    #or...
    easy_install blitzdb

For more detailed installation instructions, have a look at the [documentation](http://blitzdb.readthedocs.org).

## Detailed Documentation

The detailed documentation for this project is hosted on [ReadTheDocs](http://blitzdb.readthedocs.org), feel free to take a look!

## Changelog

* 0.4.4: SQL backend: Do not coerce server_default values via a CAST, as this can cause incompatibilities.
* 0.4.3: Many small improvements to the SQL backend.
* 0.3.0: Fully functional SQL backend.
* 0.2.12: Added support for proper attribute iteration to `Document`.
* 0.2.11: Allow setting the `collection` parameter through a `Document.Meta` attribute.
* 0.2.10: Bugfix-Release: Fix Python 3 compatibility issue.
* 0.2.9: Bugfix-Release: Fix serialization problem with file backend.
* 0.2.8: Added `get`, `has_key` and `clear` methods to `Document` class
* 0.2.7: Fixed problem with __unicode__ function in Python 3.
* 0.2.6: Bugfix-Release: Fixed an issue with the $exists operator for the file backend.
* 0.2.5: Bugfix-Release
* 0.2.4: Added support for projections and update operations to the MongoDB backend.
* 0.2.3: Bugfix-Release: Fixed bug in transaction data caching in MongoDB backend.
* 0.2.2: Fix for slice operators in MongoDB backend.
* 0.2.1: Better tests.
* 0.2.0: Support for including additional information in DB references. Support for accessing document attributes as dictionary items.
         Added $regex parameter that allows to use regular expressions in queries.
* 0.1.5: MongoDB backend now supports database transactions. Database operations are now read-isolated by default, i.e.
         uncommitted operations will not affect database queries before they are committed.
* 0.1.4: Improved indexing of objects for the file backend, added support for automatic serialization/deserialization
         of object attributes when adding keys to or querying an index.
* 0.1.3: Sorting of query sets is now supported (still experimental)
* 0.1.2: Small bugfixes, BlitzDB version number now contained in DB config dict
* 0.1.1: BlitzDB is now Python3 compatible (thanks to David Koblas)

## Contributors (in alphabetical order)

*  @bwiessneth
*  Florian Lehmann - @cashaddy
*  Karskrin - @cBrauge
*  Chris Mutel - @cmutel
*  Cecil Woebker - @cwoebker
*  Ethan Blackburn - @EthanBlackburn
*  Javier Collado - @jcollado
*  Jason Xie - @jxieeducation
*  David Koblas - @koblas
*  Stéphane Wirtel - @matrixise
*  Victor Miclovich - @miclovich
*  Dmytro Kyrychuk - @orgkhnargh
*  Christoph Neumann - @programmdesign
*  Dale - @puredistortion
*  tjado - @tejado
*  Thomas Ballinger - @thomasballinger
*  Tyler Kennedy - @TkTech
*  Toby Champion - @tobych

Thanks for all your contributions, without you BlitzDB wouldn't be what it is today :)

## Third-Party Contributions

* [Flask-BlitzDB](https://github.com/puredistortion/flask-blitzdb) Flask adapter for BlitzDB. Blitz + Flask = Awesome!

## Examples

To get an idea of what you can do with Blitz, here are some examples.

### Creating objects

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

### Storing objects in the database:

```python
from blitzdb import FileBackend

backend = FileBackend("/path/to/my/db")

the_godfather.save(backend)
marlon_brando.save(backend)
al_pacino.save(backend)
```

### Retrieving objects from the database:

```python
the_godfather = backend.get(Movie,{'pk':1L})
#or...
the_godfather = backend.get(Movie,{'name' : 'The Godfather'})
```

### Filtering objects

```python
movies_from_1972 = backend.filter(Movie,{'year' : 1972})
```

### Working with transactions

```python
backend.begin()
the_godfather.director = 'Roland Emmerich' #oops...
the_godfather.save()
backend.rollback() #undo the changes...
```

### Creating nested object references

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

### Creation of database indexes and advanced querying

```python
backend.create_index(Actor,'performances')
#Will create an index on the 'performances' field, for fast querying

godfather_cast = backend.filter(Actor,{'movies' : the_godfather})
#Will return 'Al Pacino' and 'Marlon Brando'
```

### Arbitrary filter expressions

```python
star_wars_iv = Movie({'name' : 'Star Wars - Episode IV: A New Hope','year': 1977})
star_wars_iv.save()

movies_from_the_seventies = backend.filter(Movie,{'year': lambda year : year >= 1970 and year < 1980})
#Will return Star Wars & The Godfather (man, what a decade!)
```
