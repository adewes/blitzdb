
Query Sets
==========

Query sets are used to work with sets of objects that have been retrieved from the database. For example, whenever you call the `filter` function of the backend, you will receive a `QuerySet` object in return. This object stores
references to all documents matching your query and can retrieve these objects for you if necessary.

This class is an abstract base class that gets implemented by the specific backends. 

.. autoclass:: blitzdb.queryset.QuerySet
   :members: delete, filter, __getitem__, __eq__, __ne__, __len__