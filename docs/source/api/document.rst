
The Document
============

The `Document` class is the base for all documents stored in the database. To create a new document type, just
create a class that inherits from this base class:

.. code-block:: python
   
    from blitzdb import Document

    class Author(Document):
        pass


From document classes to collections
------------------------------------

Internally, Blitz stores document attributes in *collections*, which it distinguishes based by name. By default,
the name of a collection will be the lowercased name of the corresponding class (e.g. `author` for the `Author` class above). You can override this behavior by setting the `collection` attribute in the document class' `Meta`
class attribute:

.. code-block:: python

    class Author(Document):

        class Meta(Document.Meta):
            collection = 'fancy_authors'

Likewise, you can also change the name of the attribute to be used as primary key for this document class,
which defaults to `pk`:

.. code-block:: python

    class Author(Document):

        class Meta(Document.Meta):
            pk = 'name' #use the name of the author as the primary key


.. autoclass:: blitzdb.document.Document
    :members: initialize, pk, save, delete, revert, attributes, autogenerate_pk, __eq__