
The MongoDB Backend
===============================

This backend provides a thin wrapper around MongoDB. It uses `pymongo <http://api.mongodb.org/python/2.7rc0/>`_ for the communication with MongoDB. Use this backend if you need high performance or expect to have a large number of documents (> 100.000) in your database.

.. warning::
    
    Currently this backend does not support **database transactions**. Calls to `commit` and `begin` will be silently discarded (to maintain compatibility to transactional codes), whereas a call to `rollback` will raise a :py:class:`NotInTransaction <blitzdb.backends.base.NotInTransaction>` exception.


.. autoclass:: blitzdb.backends.mongo.Backend
   :members: filter