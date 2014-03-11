Exceptions
==========

When things go awry, Blitz will throw a number of specific exceptions to indicate the
type of error that has happened. For most database operations, the exception model
is modeled after the one used in the **Django** web development framework.

Querying Errors
---------------

.. class:: blitzdb.document.Document.DoesNotExist

    Gets raised when you try to retrieve an object from the database (typically using
    the `get` function of the backend) that does not exist.

    .. admonition:: Keep in Mind

        Like the `MultipleObjectsReturned` exception, this exception is specific to
        the document class for which it is raised.

    example::

        class Author(Document):
            pass

        class Book(Document):
            pass

        try:
            raise Book.DoesNotExist
        except Author.DoesNotExist:
            #this will NOT catch the Book.DoesNotExist exception!
            print "got Author.DoesNotExist exception!" 
        except Book.DoesNotExist:
            print "got Book.DoesNotExist exception!"

.. class:: blitzdb.document.Document.MultipleObjectsReturned

    Gets raised when a query that should return only a single object (e.g. the `get` function of the backend)
    finds more than one matching document in the database. Like the `DoesNotExist` exception, it is specific
    to the document class for which it is raised.


Transaction Errors
------------------

Transaction errors get raised if functions that are supposed to run only inside a transaction get called outside of
a transaction, or vice-versa. Please note that not all backends currently support database transactions.

.. autoclass:: blitzdb.backends.base.NotInTransaction
.. autoclass:: blitzdb.backends.base.InTransaction
