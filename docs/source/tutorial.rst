:orphan:

.. _tutorial:

.. title:: Tutorial

.. role:: raw-html(raw)
   :format: html

:raw-html:`<i class="fa fa-2x fa-info-circle" style="color:#05a; vertical-align:middle; padding-right:10px;"></i>` Tutorial
======================================================================================================================================

Welcome! This tutorial will help you to get up & running with Blitz. For a more comprehensive overview of Blitz, please consult the :doc:`API documentation <api/index>`. Let's get started!

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

To store documents in a database, you need to create a :doc:`backend <api/backend>` first. Blitz supports multiple backends (currently a file-based backend and a MongoDB), in this tutorial we will use the file-based backend:

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

Retrieving Documents
--------------------

Retrieving objects from the database is just as easy. If we want to retrieve a single object, we can use the :py:meth:`get() <blitzdb.backends.base.Backend.get>` method, specifying the Document class and the properties of the document that we want to retrieve:

.. code-block:: python

    actor = backend.get(Actor,{'first_name' : 'Charlie','last_name' : 'Chaplin'})

Alternatively, if we know the `primary key` of the object, we can just specify this:

.. code-block:: python

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

Advanced Querying
^^^^^^^^^^^^^^^^^

Like MongoDB, Blitz supports advanced query operators like `$and`, `$or`, `$not`, `$in`, `$all`, `$lt`, `$ne`, ... For more information about these operators, check out the :doc:`QuerySet documentation <api/queryset>`.

Defining Relationships
----------------------

.. code-block:: python

    modern_times = Movie({
                          'title' : 'Modern Times',
                          'year' : 1936,
                          'budget' : 1500000,
                          'run_time_minutes' : 87,
                         })


Lazy Loading
^^^^^^^^^^^^

Where to Go from Here
---------------------

