from blitzdb.backends.file.queryset import QuerySet
from blitzdb.backends.file.store import TransactionalStore,Store
from blitzdb.backends.file.index import TransactionalIndex,Index
from blitzdb.backends.base import Backend as BaseBackend,NotInTransaction,InTransaction
from blitzdb.backends.file.serializers import PickleSerializer as Serializer
from blitzdb.backends.file.queries import compile_query

import os
import os.path

import hashlib
import datetime
import uuid
import copy

from collections import defaultdict

class DatabaseIndexError(BaseException):
    """
    Gets raised when the index of the database is corrupted (ideally this should never happen).

    To recover from this error, you can call the `rebuild_index` function of the file backend with the
    affected collection and key as parameters.
    """

class Backend(BaseBackend):

    """
    The file backend that stores and retrieves DB objects in files.

    To do:

    -Make storage engine for collection objects and indexes configurable
    -Make serializers for collection objects and indexes configurable

    """

    class Meta(object):
        supports_indexes = True
        supports_transactions = True

    #The default store & index classes that the backend uses
    CollectionStore = TransactionalStore
    Index = TransactionalIndex
    IndexStore = Store

    def __init__(self,path,autocommit = False,**kwargs):

        self._path = os.path.abspath(path)
        if not os.path.exists(path):
            os.makedirs(path)

        self.collections = {}
        self.stores = {}
        self.in_transaction = False
        self.autocommit = autocommit
        self.indexes = defaultdict(lambda : {})
        self.index_stores = defaultdict(lambda : {})
        self.load_config()

        super(Backend,self).__init__(**kwargs)


    def load_config(self):
        config_file = self._path+"/config.json"
        if os.path.exists(config_file):
            with open(config_file,"rb") as config_file:
                self._config = Serializer.deserialize(config_file.read())
        else:
            self._config = {
                'indexes' : {}
            }
            self.save_config()

    def save_config(self):
        config_file = self._path+"/config.json"
        with open(config_file,"wb") as config_file:
            config_file.write(Serializer.serialize(self._config))
        
    @property
    def path(self):
        return self._path

    def get_collection_store(self,collection):
        if not collection in self.stores:
            self.stores[collection] = self.CollectionStore({'path':self.path+"/"+collection+"/objects"})
        return self.stores[collection]

    def get_index_store(self,collection,store_key):
        if not store_key in self.index_stores[collection]:
            self.index_stores[collection][store_key] = self.IndexStore({'path':self.path+"/"+collection+"/indexes/"+store_key})
        return self.index_stores[collection][store_key]

    def register(self,cls,parameters = None):
        super(Backend,self).register(cls,parameters)
        self.init_indexes(self.get_collection_for_cls(cls))

    def begin(self):
        """
        Starts a new transaction
        """
        if self.in_transaction:#we're already in a transaction...
            self.commit()
        self.in_transaction = True
        for collection,store in self.stores.items():
            store.begin()
            indexes = self.indexes[collection]
            for index in indexes.values():
                index.begin()

    def rollback(self):
        """
        Rolls back a transaction
        """
        if not self.in_transaction:
            raise NotInTransaction
        for collection,store in self.stores.items():
            store.rollback()
            indexes = self.indexes[collection]
            indexes_to_rebuild = []
            for key,index in indexes.items():
                try:
                    index.rollback()
                except NotInTransaction:
                    #this index is "dirty" and needs to be rebuilt (probably it has been created within a transaction)
                    indexes_to_rebuild.append(key)
            if indexes_to_rebuild:
                self.rebuild_indexes(collection,indexes_to_rebuild)
        self.in_transaction = False

    def get_storage_key_for(self,obj):
        collection = self.get_collection_for_obj(obj)
        pk_index = self.get_pk_index(collection)
        try:
            return pk_index.get_keys_for(obj.pk)[0]
        except KeyError:
            raise obj.DoesNotExist

    def commit(self):
        """
        Commits a transaction
        """
        for collection in self.collections:
            store = self.get_collection_store(collection)
            store.commit()
            indexes = self.get_collection_indexes(collection)
            for index in indexes.values():
                index.commit()
        self.in_transaction = False
        self.begin()

    def init_indexes(self,collection):
        if collection in self._config['indexes']:
            #If not pk index is present, we create one on the fly...
            if not [idx for idx in self._config['indexes'][collection].values() if idx['key'] == self.primary_key_name]:
                self.create_index(collection,{'key':self.primary_key_name})
            
            #We sort the indexes such that pk is always created first...
            for index_params in sorted(self._config['indexes'][collection].values(),key = lambda x: 0 if x['key'] == self.primary_key_name else 1):
                index = self.create_index(collection,index_params)
        else:
            #If no indexes are given, we just create a primary key index...
            self.create_index(collection,{'key':self.primary_key_name})

    
    def rebuild_index(self,collection,key):
        return self.rebuild_indexes(collection,[key])

    def rebuild_indexes(self,collection,keys):
        if not keys:
            return
        all_objects = self.filter(collection,{})
        for key in keys:
            index = self.indexes[collection][key]
            index.clear()
        for obj in all_objects:
            serialized_attributes = self.serialize(obj.attributes)#optimize this!
            for key in keys:
                index = self.indexes[collection][key]
                index.add_key(serialized_attributes,obj._store_key)
        if self.autocommit:
            self.commit()

    def create_index(self,cls_or_collection,params,ephemeral = False):
        return self.create_indexes(cls_or_collection,[params],ephemeral = ephemeral)

    def create_indexes(self,cls_or_collection,params_list,ephemeral = False):
        indexes = []
        keys = []

        if not params_list:
            return

        if not isinstance(cls_or_collection,str) and not isinstance(cls_or_collection,unicode):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection

        for params in params_list:
            if not isinstance(params,dict):
                params = {'key' : params}
            if params['key'] in self.indexes[collection]:
                return #Index already exists
            if not 'id' in params:
                params['id'] = uuid.uuid4().hex 
            if ephemeral:
                index_store = None
            else:
                index_store = self.get_index_store(collection,params['id'])
            index = self.Index(params,index_store)
            self.indexes[collection][params['key']] = index

            if not collection in self._config['indexes']:
                self._config['indexes'][collection] = {}

            self._config['indexes'][collection][params['key']] = params
            self.save_config()
            indexes.append(index)
            if not index.loaded:#if the index failed to load from disk we rebuild it
                keys.append(params['key'])

        self.rebuild_indexes(collection,keys)

        return indexes

    def get_collection_indexes(self,collection):
        return self.indexes[collection] if collection in self.indexes else {}

    def encode_attributes(self,attributes):
        return Serializer.serialize(attributes)

    def decode_attributes(self,data):
        return Serializer.deserialize(data)

    def get_object(self,cls,key):
        collection = self.get_collection_for_cls(cls)
        store = self.get_collection_store(collection)
        try:
            data = self.deserialize(self.decode_attributes(store.get_blob(key)))
        except IOError:
            raise cls.DoesNotExist
        obj = self.create_instance(cls,data)
        return obj

    def save(self,obj):
        collection = self.get_collection_for_obj(obj)
        indexes = self.get_collection_indexes(collection)
        store = self.get_collection_store(collection)

        if obj.pk == None:
            obj.autogenerate_pk()

        serialized_attributes = self.serialize(obj.attributes)
        data = self.encode_attributes(serialized_attributes)
    
        try:
            store_key = self.get_pk_index(collection).get_keys_for(obj.pk).pop()
        except IndexError:
            store_key = uuid.uuid4().hex
    
        store.store_blob(data,store_key)

        for key,index in indexes.items():
            index.add_key(serialized_attributes,store_key)

        if self.autocommit:
            self.commit()

        return obj

    def get_pk_index(self,collection):
        if not self.primary_key_name in self.indexes[collection]:
            self.create_index(key,collection)
        return self.indexes[collection][self.primary_key_name]

    def delete_by_store_keys(self,collection,store_keys):

        store = self.get_collection_store(collection)
        indexes = self.get_collection_indexes(collection)     

        for store_key in store_keys:
            try:
                store.delete_blob(store_key)
            except (KeyError,IOError):
                pass
            for index in indexes.values():
                index.remove_key(store_key)
        
        if self.autocommit:
            self.commit()

    def delete(self,obj):        
        collection = self.get_collection_for_obj(obj)
        primary_index = self.get_pk_index(collection)
        return self.delete_by_store_keys(collection,primary_index.get_keys_for(obj.pk))

    def get(self,cls,query):
        objects = self.filter(cls,query,limit = 1)
        if len(objects) == 0:
            raise cls.DoesNotExist
        elif len(objects) > 1:
            raise cls.MultipleDocumentsReturned
        return objects[0]

    def filter(self,cls_or_collection,query,sort_by = None,limit = None,offset = None,initial_keys = None):

        if not isinstance(query,dict):
            raise AttributeError("Query parameters must be dict!")

        if not isinstance(cls_or_collection,str) and not isinstance(cls_or_collection,unicode):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        store = self.get_collection_store(collection)
        indexes = self.get_collection_indexes(collection)
        compiled_query = compile_query(self.serialize(query))

        indexes_to_create = []

        def query_function(key,expression):
            if key == None:
                return QuerySet(self,cls,store,self.get_pk_index(collection).get_all_keys())
            qs =  QuerySet(self,cls,store,indexes[key].get_keys_for(expression))
            return qs

        def index_collector(key,expressions):
            if key not in indexes and key not in indexes_to_create and key != None:
                indexes_to_create.append(key)
            return QuerySet(self,cls,store,[])

        #We collect all the indexes that we need to create
        compiled_query(index_collector)
        self.create_indexes(cls,indexes_to_create)

        return compiled_query(query_function)

