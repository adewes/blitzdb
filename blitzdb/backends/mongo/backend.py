import abc

from blitzdb.object import Object
from blitzdb.backends.base import Backend as BaseBackend
from blitzdb.backends.mongo.queryset import QuerySet
import uuid

class Backend(BaseBackend):

    """
    """

    def __init__(self,db):
        super(Backend,self).__init__()
        self.db = db
        self.classes = {}
        self.collections = {}

    def begin(self):
        """
        Starts a new transaction
        """
        pass
#        raise Exception("Mongo backend does not support transactions!")

    def rollback(self):
        """
        Rolls back a transaction
        """
        pass
#        raise Exception("Mongo backend does not support transactions!")

    def commit(self):
        """
        Commits a transaction
        """
        pass
#        raise Exception("Mongo backend does not support transactions!")

    def get(self,cls_or_collection,properties):
        if not isinstance(cls_or_collection,str) and not isinstance(cls_or_collection,unicode):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        obj = self.db[collection].find_one(properties)
        if not obj:
            cls = self.get_cls_for_collection(collection)
            raise cls.DoesNotExist
        return obj

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

    def ensure_index(self,cls_or_collection,*args,**kwargs):
        if not isinstance(cls_or_collection,str) and not isinstance(cls_or_collection,unicode):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        self.db[collection].ensure_index(*args,**kwargs)

    def filter(self,cls_or_collection,query,sort_by = None,limit = None,offset = None):

        if not isinstance(cls_or_collection,str) and not isinstance(cls_or_collection,unicode):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        compiled_query = self.serialize(query)

        return QuerySet(self,cls,self.db[collection].find(compiled_query))