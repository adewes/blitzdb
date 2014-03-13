
The Native (file-based) Backend
===============================

Stores documents and indexes in flat files on your disk. Can be used without any external software, so it's a great fit for projects that need a document-oriented database but do not want (or are unable) to use a third-party solution for this.

.. note::

    This backend is **transactional**, which means that changes on the database will be written to disk only when you call the :py:meth:`.Backend.commit` function explicitly (there is an `autocommit` option, though).

The performance of this backend is reasonable for moderately sized datasets (< 100.000 entries).Future version of the backend might support in-memory caching of objects to speed up the performance even more.


.. autoclass:: blitzdb.backends.file.Backend
    :show-inheritance:
    :members: rollback, commit, rebuild_index, create_index, begin