
Backends
========

Under the hood, BlitzDB is not just a database engine but more a **database wrapper** like **SQLAlchemy**. Since it provides
its own file-based backend, it can be used as a standalone solution though. In some cases it might be useful to use it with
a third-party backend such as MongoDB though, e.g. if you need more "*horse power*" or want the additional query efficiency that real databases usually offer.


Currently, Blitz comes with two preinstalled backends:

.. toctree::
    :hidden:
    :maxdepth: 0

    Native <file>
    MongoDB <mongo>


* :doc:`Native Backend <file>` The **native backend**, which we sometimes refer to as the **file-based backend** uses a file-based index and flat files to store objects in a local directory. It has not any external dependencies and is usually sufficent for most low- to mid-end applications.

* :doc:`MongoDB Backend <mongo>` The **MongoDB backend** uses `PyMongo <http://api.mongodb.org/python/2.7rc0/>`_ to store and retrieve documents from a MongoDB database. It can be used in high-end applications, where use of a professional database engine is advocated.

