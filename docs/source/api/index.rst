
API
===

The architecture of BlitzDB is defined by the following three classes:

* :doc:`Document <document>` : The `Document` class is the base class for all documents stored in a database. 
* :doc:`Backend <backend>` : The `Backend` class is responsible for storing and retrieving documents from a database. 
* :doc:`QuerySet <queryset>` : The `QuerySet` class manages a list of documents as returned e.g. by the :py:meth:`filter <blitzdb.backends.base.Backend.filter>` function. 

In addition, specific backends might internally define further classes, with which you will normally not interact as an end user, though.

.. toctree::
    :hidden:
    :maxdepth: 2

    Backend <backend>
    Document <document>
    QuerySet <queryset>
    Exceptions <exceptions>
