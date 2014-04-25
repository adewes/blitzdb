import abc
import six

from collections import defaultdict

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

    #magic value to replace '.' characters in dictionary keys (which breaks MongoDB)
    DOT_MAGIC_VALUE = ":a5b8afc131:"

    def __init__(self,db,autocommit = False,**kwargs):
        self.db = db
        self.classes = {}
        self.collections = {}
        self._autocommit = autocommit
        self._save_cache = defaultdict(lambda  : {})
        self._delete_cache = defaultdict(lambda : {})
        self.in_transaction = False
        super(Backend,self).__init__(**kwargs)

    def begin(self):
        if self.in_transaction:#we're already in a transaction...
            self.commit()
        self.in_transaction = True

    def rollback(self):
        if not self.in_transaction:
            raise NotInTransaction("Not in a transaction!")
        self._write_cache = defaultdict(lambda : {})
        self._delete_cache = defaultdict(lambda : {})
        self.in_transaction = False

    def commit(self):
        for collection,cache in self._save_cache.items():
            for pk,attributes in cache.items():
                self.db[collection].save(attributes)
        for collection,cache in self._delete_cache.items():
            for pk in cache:
                self.db[collection].remove({'_id' : pk})

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self,value):
        if not value in (True,False):
            raise TypeError("Value must be boolean!")
        self._autocommit = value

    def delete_by_primary_keys(self,cls,pks):
        collection = self.get_collection_for_cls(cls)
        if self.autocommit:
            for pk in pks:
                self.db[collection].remove({'_id' : pk})
        else:
            self._delete_cache[collection].update(dict([(pk,True) for pk in pks]))

    def delete(self,obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            raise obj.DoesNotExist
        if self.autocommit:
            self.db[collection].remove({'_id' : obj.pk})
        else:
            self._delete_cache[collection][obj.pk] = True
            if obj.pk in self._save_cache[collection]:
                del self._save_cache[collection][obj.pk]            

    def save_multiple(self,objs):
        if not objs:
            return
        serialized_attributes_list = []
        collection = self.get_collection_for_cls(objs[0].__class__)
        for obj in objs:
            if obj.pk == None:
                obj.pk = uuid.uuid4().hex
            serialized_attributes = self.serialize(obj.attributes)
            serialized_attributes['_id'] = obj.pk
            serialized_attributes_list.append(serialized_attributes)
        for attributes in serialized_attributes_list:
            if self.autocommit:
                self.db[collection].save(attributes)
            else:
                self._save_cache[collection][attributes['pk']] = attributes
                if attributes['pk'] in self._delete_cache[collection]:
                    del self._delete_cache[collection][attributes['pk']]

    def save(self,obj):
        return self.save_multiple([obj])

    def serialize(self,obj,convert_keys_to_str = True,embed_level = 0,encoders = None,autosave = True):

        def encode_dict(obj):
            return dict([(key.replace(".",self.DOT_MAGIC_VALUE),value) for key,value in obj.items()])

        dict_encoders = [(lambda obj:True if isinstance(obj,dict) else False,encode_dict)]
        return super(Backend,self).serialize(obj,convert_keys_to_str = convert_keys_to_str,embed_level = embed_level, encoders = encoders + dict_encoders if encoders else dict_encoders,autosave = autosave)

    def deserialize(self,obj,decoders = None):

        def decode_dict(obj):
            return dict([(key.replace(self.DOT_MAGIC_VALUE,"."),value) for key,value in obj.items()])

        dict_decoders = [(lambda obj:True if isinstance(obj,dict) and '_type' in obj and obj['_type'] == 'dict' and 'items' in obj else False,decode_dict)]
        return super(Backend,self).deserialize(obj,decoders = dict_decoders + decoders if decoders else dict_decoders)

    def create_indexes(self,cls_or_collection,params_list):
        for params in params_list:
            self.create_index(cls_or_collection,*params)

    def create_index(self,cls_or_collection,*args,**kwargs):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        self.db[collection].ensure_index(*args,**kwargs)

    def compile_query(self,query):
        if isinstance(query,dict):
            return dict([(self.compile_query(key),self.compile_query(value)) for key,value in query.items()])
        elif isinstance(query,list) or isinstance(query,QuerySet) or isinstance(query,tuple):
            return  [self.compile_query(x) for x in query]
        else:
            return self.serialize(query,autosave = False)

    def get(self,cls_or_collection,properties,raw = False):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        cls = self.get_cls_for_collection(collection)
        queryset = self.filter(cls_or_collection,properties,raw = raw)
        if len(queryset) == 0:
            raise cls.DoesNotExist
        elif len(queryset) > 1:
            raise cls.MultipleDocumentsReturned
        return queryset[0]

    def filter(self,cls_or_collection,query,sort_by = None,limit = None,offset = None,raw = False):
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

        return QuerySet(self,cls,self.db[collection].find(compiled_query),raw = raw)
