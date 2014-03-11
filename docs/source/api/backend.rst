
The Backend
===========

The backend provides the main interface to the database and is responsible for retrieving, storing
and deleting objects. The class documented here is an abstract base class that gets implemented
by the specific backends. Functionality can vary between different backends, e.g. **database transactions**
will not be supported by all backends.

.. autoclass:: blitzdb.backends.base.Backend
   :members: register, autodiscover_classes, autoregister, serialize, deserialize, save, get, filter, delete