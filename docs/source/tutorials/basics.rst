:orphan:

.. _basics:

.. title:: BlitzDB Basics

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
                             'birth_year' : 1889,
                             'filmography' : [
                                ('The Kid',1921),
                                ('A Woman of Paris',1923),
                                #...
                                ('Modern Times', 1936)
                             ]
                            })

We can access the document attributes of the given instances as class attributes:

.. code-block:: python

    print "%s %s was born in %d" % (charlie_chaplin.first_name,
                                    charlie_chaplin.last_name,
                                    charlie_chaplin.birth_year)

Alternatively, we can use the `attributes` attribute to access them:

.. code-block:: python

    print "%(first_name)s %(last_name)s was born in %(birth_year)d" % charlie_chaplin.attributes

This is also pretty useful if you define attributes that have names which get *shadowed* by methods of the `Document` class (e.g. `save` or `filter`).


Connecting to a database
------------------------

To store documents in a database, you need to create a :doc:`backend </api/backend>` first. Blitz supports multiple backends (currently a file-based one and one that wraps MongoDB). In this tutorial we will use a file-based backend, which you create like this:

.. code-block:: python

    from blitzdb import FileBackend

    backend = FileBackend("./my-db")

This connects Blitz to a file-based database within the "./my-db" directory, or creates a new database there
if none should be present. The backend provides various functions such as :py:meth:`save <blitzdb.backends.base.Backend.save>`, :py:meth:`get <blitzdb.backends.base.Backend.get>`, :py:meth:`filter <blitzdb.backends.base.Backend.filter>` and :py:meth:`delete <blitzdb.backends.base.Backend.delete>`, which can be used to store, retrieve, update and delete objects. Let's have a look at these operations.

.. note::

   You can choose between different formats to store your documents when using the file-based backend, using e.g. the `json`, `pickle` or `marshal` Python libraries. By default, all documents will be stored as gzipped JSON files.

Inserting Documents
-------------------

We can store the `Author` object that we created before in our new database like this:

.. code-block:: python

    backend.save(charlie_chaplin)

Alternatively, we can also directly call the `save` function of the `Actor` instance with the backend as an argument:

.. code-block:: python

    charlie_chaplin.save(backend)

In addition, since Blitz is a **transactional database**, we have to call the :py:meth:`commit <blitzdb.backends.file.Backend.commit>` function of the backend to write the new document to disk:

.. code-block:: python

    #Will commit changes to disk
    backend.commit()

.. note:: 

    Use the :py:meth:`Backend.begin <blitzdb.backends.file.Backend.begin>` function to start a new database transaction and the :py:meth:`Backend.rollback <blitzdb.backends.file.Backend.rollback>` function to roll back the state of the database to the beginning of a transaction, if needed. By default, Blitz uses a **local isolation level** for transactions, so changes you make to the state of the database will be visible to parts of your program using the same backend, but will only be written to disk when :py:meth:`Backend.commit <blitzdb.backends.file.Backend.commit>` is invoked. 

Retrieving Documents
--------------------

Retrieving objects from the database is just as easy. If we want to get a single object, we can use the :py:meth:`get() <blitzdb.backends.base.Backend.get>` method, specifying the Document class and any combination of attributes that uniquely identifies the document:

.. code-block:: python

    actor = backend.get(Actor,{'first_name' : 'Charlie','last_name' : 'Chaplin'})

Alternatively, if we know the `primary key` of the object, we can just specify this:

.. code-block:: python

    the_kid = Movie({'title' : 'The Kid'})
    actor = backend.get(Actor,{'pk' : charlie_chaplin.pk})

.. note::

    **Pro-Tip**

    If Blitz can't find a document matching your query, it will raise a :py:class:`Document.DoesNotExist <blitzdb.document.Document.DoesNotExist>` exception. Likewise, if it finds more than one document matching your query it will raise :py:class:`Document.MultipleObjectsReturned <blitzdb.document.Document.MultipleObjectsReturned>`. These exceptions are specific to the document class to which they belong and can be accessed as attributes of it, e.g. like this: 

    .. code-block:: python

        try:
            actor = backend.get(Actor,{'first_name' : 'Charlie'})
        except Actor.DoesNotExist:
            #no 'Charlie' in the database
            pass
        except Actor.MultipleObjectsReturned:
            #more than one 'Charlie' in the database
            pass

If we want to retrieve all objects matching a given query, we can use the :py:meth:`filter() <blitzdb.backends.base.Backend.filter>` method instead:

.. code-block:: python

    #Retrieve all actors that were born in 1889
    actors = backend.filter(Actor,{'birth_year' : 1889})

This will return a :py:class:`QuerySet <blitzdb.queryset.QuerySet>`, which contains a list of keys of all objects that match our query. Query sets are iterables, so we can use them just like lists:

.. code-block:: python

    print "Found %d actors" % len(actors)
    for actor in actors:
        print actor.first_name+" "+actor.last_name

Deleting Documents
------------------

We can delete documents from the database by calling the :py:meth:`delete() <blitzdb.backends.base.Backend.delete>` method of the backend with an instance of the object that we wish to delete:

.. code-block:: python

    backend.delete(charlie_chaplin)

This will remove the document from the given collection and set its primary key to `None`. We can delete a whole query set in the same way by calling its :py:meth:`delete() <blitzdb.queryset.QuerySet.delete>` method:

.. code-block:: python

    #Retrieve all actors from the database
    actors = backend.filter(Actor,{})
    actors.delete()

Defining Relationships
----------------------

Databases are pretty useless if there's no way to define **relationships** between objects. Like MongoDB, Blitz supports defining references to other documents inside of documents. An example:

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

Internally, BlitzDB converts any `Document` instance that it encounters inside a document to a database reference that contains the primary key of the embedded document and the the name of the collection in which it is stored. Like this, if we reload the actor from the database, the embedded movie objects will get automatically (lazy-)loaded as well:

.. code-block:: python

    actor = backend.filter(Actor,{'first_name' : 'Charlie','last_name' : 'Chaplin'})

    #check that the movies in the retrieved Actor document are instances of Movie
    assert isinstance(actor.movies[0],Movie)

    #will print 'Modern Times'
    print actor.movies[0].title 

.. note::

    When an object gets loaded from the database, references to other objects that it contains will get loaded **lazily**, i.e. they will get initialized with only their primary key and the name of the collection they can be found in. Their attributes will get automatically loaded if (and only if) you should request them. 

    Like this, Blitz avoids performing multiple reads from the database unless they are really needed. As a bonus, lazy loading also solves the problem of cyclic document references (like in the example above).

Advanced Querying
-----------------

Like MongoDB, Blitz supports advanced query operators, which you can include in your query be prefixing them with a `$`. Currently, the following operator expressions are supported: 

* **$and** : Performs a boolean **AND** on two or more expressions
* **$or** : Performs a boolean **OR** on two or more expressions
* **$gt** : Performs a **>** comparision between an attribute and a specified value
* **$gte** : Performs a **>=** comparision between an attribute and a specified value
* **$lt** : Performs a **<** comparision between an attribute and a specified value
* **$lte** : Performs a **<=** comparision between an attribute and a specified value
* **$all** : Returns documents containing all values in the argument list.
* **$in** : Returns documents matching at least one of the values in the argument list.
* **$ne** : Performs a **not equal** operation on the given expression
* **$not** : Checks for non-equality between an attribute and the given value.

The syntax and semantics of these operators is identical to MongoDB, so for further information have a look at `their documentation <http://docs.mongodb.org/manual/reference/operator/query/>`_.

Example: Boolean AND
^^^^^^^^^^^^^^^^^^^^

By default, if you specify more than one attribute in a query, an implicit `$and` query will be performed, returning only the documents that match **all** attribute/value pairs given in your query. You can also specify this behavior explicitly by using then `$and` operator, so the following two queries are identical:

.. code-block:: python

    backend.filter(Actor,{'first_name' : 'Charlie','last_name' : 'Chaplin'})
    #is equivalent to...
    backend.filter(Actor,{'$and' : [{'first_name' : 'Charlie'},{'last_name' : 'Chaplin'}]})

Using `$and` can be necessary if you want to reference the same document attribute more than once in your query, e.g. like this:

.. code-block:: python

    #Get all actors born beteen 1900 and 1940
    backend.filter(Actor,{'$and' : [{'birth_year' : {'$gte' : 1900}},{'birth_year' : {'$lte' : 1940}}]})

Where to Go from Here
---------------------

Currently there are no other tutorials available (this will change soon), so if you have further questions, feel free to `send us an e-mail <mailto:andreas@7scientists.com>`_ or post an `issue on Github <https://github.com/adewes/blitzdb/issues>`_. The `test suite <https://github.com/adewes/blitzdb/tree/master/blitzdb/tests>`_ also contains a large number of examples on how to use the API to work with documents.