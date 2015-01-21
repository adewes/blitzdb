import abc
import six

from blitzdb.document import Document
from blitzdb.backends.base import Backend as BaseBackend
from blitzdb.backends.base import NotInTransaction
from blitzdb.backends.mongo.queryset import QuerySet
import uuid

"""
Base model for SQL backend:

Data storage:

Define a JSON column in th underlying database

Indexes:

Define additional columns in the table with a given type

M2M-Relationships: Let the user define them through helper documents
"""


class Backend(BaseBackend):

    """
    A SQL backend.

    :param db: An instance of a `sqlalchemy. 
    <http://www.sqlalchemy.org>`_ class

    Example usage:

    .. code-block:: python

        from sqlalchemy import
        from blitzdb.backends.sql import Backend as SQLBackend

        my_db = ...

        #create a new BlitzDB backend using a SQLAlchemy database
        backend = SQLBackend(my_db)
    """

    def __init__(self, db, **kwargs):
        self.db = db
        self.classes = {}
        self.collections = {}
        super(Backend, self).__init__(**kwargs)

    def begin(self):
        pass

    def rollback(self):
        raise NotInTransaction("SQLAlchemy backend does not support rollback!")

    def commit(self):
        pass

    def get(self, cls_or_collection, properties):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        #...

    def delete(self, obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            raise obj.DoesNotExist
        #...

    def save(self, obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            obj.pk = uuid.uuid4().hex
        serialized_attributes = self.serialize(obj.attributes)
        serialized_attributes['_id'] = obj.pk
        #...

    def serialize(self, obj, convert_keys_to_str=True, embed_level=0, encoders=None):
        return super(Backend, self).serialize(obj, 
                                              convert_keys_to_str=convert_keys_to_str, 
                                              embed_level=embed_level, 
                                              encoders=encoders)

    def deserialize(self, obj, decoders=None):
        return super(Backend, self).deserialize(obj, decoders=decoders)

    def create_index(self, cls_or_collection, *args, **kwargs):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        self.db[collection].ensure_index(*args, **kwargs)

    def compile_query(self, query):
        if isinstance(query, dict):
            return dict([(self.compile_query(key), self.compile_query(value)) 
                         for key, value in query.items()])
        elif isinstance(query, list):
            return [self.compile_query(x) for x in query]
        else:
            return self.serialize(query)

    def filter(self, cls_or_collection, query, sort_by=None, limit=None, offset=None):
        """
        Filter objects from the database that correspond to a given set of properties.

        See :py:meth:`blitzdb.backends.base.Backend.filter` for documentation of individual parameters

        .. note::

            This function supports all query operators that are available in SQLAlchemy and returns a query set
            that is based on a SQLAlchemy cursor.
        """

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        compiled_query = self.compile_query(query)

        return QuerySet(self, cls, self.db[collection].find(compiled_query))
