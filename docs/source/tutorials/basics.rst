:orphan:

.. _basics:

.. title:: Basics

.. role:: raw-html(raw)
   :format: html

:raw-html:`<i class="fa fa-info-circle" style="color:#05a; vertical-align:middle; padding:10px;"></i>` Basics
***************************************************************************************************************

Welcome! This tutorial will help you to get up & running with Blitz. For a more comprehensive overview of Blitz, please consult the :doc:`API documentation </api/index>` or the documentation of :doc:`specific backends </backends/index>`. Let's get started!

Working with Documents
----------------------

Just like in Python, in Blitz all documents are objects. To create a new type of document, you just define a class that derives from :py:class:`blitzdb.document.Document`:

.. code-block:: python
    
    from blitzdb import Document

    class Actor(Document):
        pass

    class Movie(Document):
        pass

That's it! We can now create and work with instances of `Actor` and `Movie` documents:

.. code-block:: python

    charlie_chaplin = Actor({
                             'first_name' : 'Charlie',
                             'last_name' : 'Chaplin', 
                             'is_funny' : True,
                             'birth_year' : 1889
                            })

We can then query the attributes of the given instances as class attributes:

.. code-block:: python

    print "%s %s was born in %d" % (charlie_chaplin.first_name,
                                    charlie_chaplin.last_name,
                                    charlie_chaplin.birth_year)

Alternatively, we can use the `attributes` variable to access the attributes dictionary of the instance:

.. code-block:: python

    print "%(first_name)s %(last_name)s was born in %(birth_year)d" % charlie_chaplin.attributes

This is also pretty useful to access attributes that have names which are *shadowed* by methods of the `Document` class (e.g. `save` or `filter`).


Connecting to a database
------------------------

To store documents in a database, you need to create a :doc:`backend </api/backend>` first. Blitz supports multiple backends (currently a file-based backend and a MongoDB), in this tutorial we will use the file-based backend:

.. code-block:: python

    from blitzdb import FileBackend

    backend = FileBackend("./my-db")

This creates a connection to a file-based database within the "./my-db" directory, or creates a new database there
if none should be present. The backend provides various functions such as :py:meth:`save <blitzdb.backends.base.Backend.save>`, :py:meth:`get <blitzdb.backends.base.Backend.get>`, :py:meth:`filter <blitzdb.backends.base.Backend.filter>` and :py:meth:`delete <blitzdb.backends.base.Backend.delete>`, which can be used to store, retrieve, update and delete objects. Let's have a look at these operations:

Inserting Documents
-------------------

We can store our `Author` object in the database like this:

.. code-block:: python

    backend.save(charlie_chaplin)

Alternatively, we could also call the `save` function of the `Actor` instance with the backend as an argument:

.. code-block:: python

    charlie_chaplin.save(backend)

.. warning::

                Some database backends (e.g. the file-based backend) will cache operations that you perform in memory
                and will only write the objects to disk when you call :py:meth:`commit <blitzdb.backends.file.Backend.commit>`.

Retrieving Documents
--------------------

Retrieving objects from the database is just as easy. If we want to retrieve a single object, we can use the :py:meth:`get() <blitzdb.backends.base.Backend.get>` method, specifying the Document class and the properties of the document that we want to retrieve:

.. code-block:: python

    actor = backend.get(Actor,{'first_name' : 'Charlie','last_name' : 'Chaplin'})

Alternatively, if we know the `primary key` of the object, we can just specify this:

.. code-block:: python

    the_kid = Movie({'title' : 'The Kid'})
    actor = backend.get(Actor,{'pk' : charlie_chaplin.pk})

If we want to retrieve more than one object at a given time, we can use the :py:meth:`filter() <blitzdb.backends.base.Backend.filter>` method:

.. code-block:: python

    #Retrieve all actors that were born in 1889
    actors = backend.filter(Actor,{'birth_year' : 1889})

This will return an instance of the :py:class:`QuerySet <blitzdb.queryset.QuerySet>` class, which contains a list of keys of the objects that match with our query. Query sets are iterables, so we can use them just like lists:

.. code-block:: python

    print "Found %d actors" % len(actors)
    for actor in actors:
        print actor.first_name+" "+actor.last_name

Deleting Documents
------------------

We can delete documents from the database by calling the :py:meth:`delete() <blitzdb.backends.base.Backend.delete>` method of the backend with an instance of the object that we wish to delete:

.. code-block:: python

    backend.delete(charlie_chaplin)

We can delete a whole query set in the same way by calling its :py:meth:`delete() <blitzdb.queryset.QuerySet.delete>` method:

.. code-block:: python

    #Retrieve all actors from the database
    actors = backend.filter(Actor,{})
    actors.delete()

Defining Relationships
----------------------

Databases are pretty useless if there's no way to define **relationships** between objects in them. Like MongoDB, BlitzDB supports defining references to other documents inside of documents. An example:

.. code-block:: python

    modern_times = Movie({
                          'title' : 'Modern Times',
                          'year' : 1936,
                          'budget' : 1500000,
                          'run_time_minutes' : 87,
                         })

    charlie_chaplin.movies = [modern_times]
    modern_times.actors = [charlie_chaplin]

    #this will automatically save the movie object as well
    backend.save(charlie_chaplin) 

Internally, BlitzDB converts any `Document` instance that it encounters inside a document to a database reference that contains the primary key of the embedded document and the the name of the collection in which it is stored. Like this, when we load the actor from the database, the embedded movie object will get automatically (lazy-)loaded as well:

.. code-block:: python

    actor = backend.filter(Actor,{'first_name' : 'Charlie','last_name' : 'Chaplin'})
    #will print 'Modern Times'
    print actor.movies[0].title 

.. note::

    When an object gets loaded from the database, references to other objects that it might contain will get **lazily loaded**, i.e. the embedded object will get initialized with only its primary key and the attributes of the object will get automatically loaded if the program requests them. Like this, BlitzDB can avoid performing multiple reads from the database unless they are really needed. As a bonus, lazy loading also solves the problem of cyclic references.

Advanced Querying
-----------------

Like MongoDB, Blitz supports advanced query operators like `$and`, `$or`, `$not`, `$in`, `$all`, `$lt`, `$ne`, ... For more information about these operators, check out the :doc:`QuerySet documentation </api/queryset>`.

Boolean Operators
^^^^^^^^^^^^^^^^^

In BlitzDB, just like in MongoDB, if you specify more than one document field in a query, an implicit `$and` query will be performed, so the two following queries are actually identical:

.. code-block:: python

    backend.filter(Actor,{'first_name' : 'Charlie','last_name' : 'Chaplin'})
    #is equivalent to...
    backend.filter(Actor,{'$and' : [{'first_name' : 'Charlie'},{'last_name' : 'Chaplin'}]})

The syntax of the other operators is identical to MongoDB, so for further information have a look at `their documentation <http://docs.mongodb.org/manual/reference/operator/query/>`_.

Where to Go from Here
---------------------

Currently there are no other tutorials available (this will change soon), so if you have further questions, feel free to `e-mail me <mailto:andreas@7scientists.com>`_ or post an `issue on Github <https://github.com/adewes/blitzdb/issues>`_. The `test suite <https://github.com/adewes/blitzdb/tree/master/blitzdb/tests>`_ also contains a large number of examples on how to use the API to work with documents.