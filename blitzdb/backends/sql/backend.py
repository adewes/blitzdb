import abc
import six

from blitzdb.document import Document
from blitzdb.backends.base import Backend as BaseBackend
from blitzdb.backends.base import NotInTransaction
from blitzdb.backends.mongo.queryset import QuerySet
import uuid

class Backend(BaseBackend):

    """
    A MongoDB backend.

    :param db: An instance of a `pymongo.database.Database <http://api.mongodb.org/python/current/api/pymongo/database.html>`_ class

    Example usage:

    .. code-block:: python

        from pymongo import connection
        from blitzdb.backends.mongo import Backend as MongoBackend

        c = connection()
        my_db = c.test_db

        #create a new BlitzDB backend using a MongoDB database
        backend = MongoBackend(my_db)
    """

    def __init__(self,db,**kwargs):
        self.db = db
        self.classes = {}
        self.collections = {}
        super(Backend,self).__init__(**kwargs)

    def begin(self):
        pass

    def rollback(self):
        raise NotInTransaction("MongoDB backend does not support rollback!")

    def commit(self):
        pass

    def get(self,cls_or_collection,properties):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        attributes = self.db[collection].find_one(self.serialize(properties))
        cls = self.get_cls_for_collection(collection)
        if not attributes:
            raise cls.DoesNotExist
        return self.create_instance(cls,self.deserialize(attributes))

    def delete(self,obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            raise obj.DoesNotExist
        self.db[collection].remove({'_id' : obj.pk})

    def save(self,obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            obj.pk = uuid.uuid4().hex
        serialized_attributes = self.serialize(obj.attributes)
        serialized_attributes['_id'] = obj.pk
        self.db[collection].save(serialized_attributes)

    def serialize(self,obj,convert_keys_to_str = True,embed_level = 0,encoders = None):
        return super(Backend,self).serialize(obj,convert_keys_to_str = convert_keys_to_str,embed_level = embed_level, encoders = encoders)

    def deserialize(self,obj,decoders = None):
        return super(Backend,self).deserialize(obj,decoders = decoders)


    def create_index(self,cls_or_collection,*args,**kwargs):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        self.db[collection].ensure_index(*args,**kwargs)

    def compile_query(self,query):
        if isinstance(query,dict):
            return dict([(self.compile_query(key),self.compile_query(value)) for key,value in query.items()])
        elif isinstance(query,list):
            return  [self.compile_query(x) for x in query]
        else:
            return self.serialize(query)

    def filter(self,cls_or_collection,query,sort_by = None,limit = None,offset = None):
        """
        Filter objects from the database that correspond to a given set of properties.

        See :py:meth:`blitzdb.backends.base.Backend.filter` for documentation of individual parameters

        .. note::

            This function supports all query operators that are available in MongoDB and returns a query set
            that is based on a MongoDB cursor.


        """

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        compiled_query = self.compile_query(query)

        return QuerySet(self,cls,self.db[collection].find(compiled_query))
