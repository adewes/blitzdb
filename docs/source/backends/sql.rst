
The SQL Backend
===============

Stores documents in an SQL database, using `SQLAlchemy` as a wrapper library.

.. note::

    This backend is **transactional**, which means that changes on the database will
    be written to disk only when you call the :py:meth:`.Backend.commit` function
    explicitly (there is an `autocommit` option, though).

This backend requires you to predefine the structure of your documents to a certain extent in order
to make queries over them and implement foreign key relationships.

You can define index fields through the document classes `Meta` class. Currently, the following
index options are supported:

* `foreign_key` : Defines a foreign key relationship to another collection. The key that is 
  referenced in the collection has to be

.. autoclass:: blitzdb.backends.sql.Backend
    :show-inheritance:
    :members: rollback, commit, rebuild_index, create_index, begin