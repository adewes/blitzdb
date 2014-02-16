from blitzdb.backends.base import Backend as BaseBackend
from blitzdb.queryset import QuerySet

import os
import os.path

import json
import hashlib
import datetime
import uuid
import copy

from collections import defaultdict

"""
Database transactions:

-Keep a log of all raw DB transactions in the cache
-When the transaction is complete, perform the actions and write indexes to disk.
-Affected transaction type: delete, save/update

Both things can be implemented by subclassing or altering the Index and Store classes.

"""

class JsonEncoder(json.JSONEncoder):
    
    def default(self, obj):
        if isinstance(obj,set):
            return list(obj)
        elif isinstance(obj,datetime.datetime):
            return obj.ctime()
        return json.JSONEncoder.default(self, obj)


class Store(object):

    """
    A store object stores and retrieves blobs.
    """

    def __init__(self,properties):
        self._properties = properties

        if not 'path' in properties:
            raise AttributeError("You must specify a path when creating a Store!")

        if not os.path.exists(properties['path']):
            os.makedirs(properties['path'])

    def _get_path_for_key(self,key):
        return self._properties['path']+'/'+key

    def store_blob(self,blob,key):
        with open(self._properties['path']+"/"+key,"w") as output_file:
            output_file.write(blob)
        return key

    def delete_blob(self,key):
        filepath = self._get_path_for_key(key)
        if os.path.exists(filepath):
            os.unlink(filepath)

    def get_blob(self,key):
        with open(self._properties['path']+"/"+key,"r") as input_file:
            return input_file.read()

    def has_blob(self,key):
        if os.path.exists(self._properties['path']+"/"+key):
            return True
        return False

class TransactionalStore(Store):

    def __init__(self,properties):
        super(TransactionalStore,self).__init__(properties)
        self.begin()

    def begin(self):
        self._delete_cache = set()
        self._update_cache = {}

    def commit(self):
        for store_key in self._delete_cache:
            super(TransactionalStore,self).delete_blob(store_key)
        for store_key,blob in self._update_cache.items():
            super(TransactionalStore,self).store_blob(blob,store_key)

    def has_blob(self,key):
        if key in self._delete_cache:
            return False
        if key in self._update_cache:
            return True
        return super(TransactionalStore,self).has_blob(key)

    def get_blob(self,key):
        if key in self._update_cache:
            return self._update_cache[key]
        return super(TransactionalStore,self).get_blob(key)

    def store_blob(self,blob,key):
        if key in self._delete_cache:
            self._delete_cache.remove(key)
        self._update_cache[key] = copy.copy(blob)
        return key

    def delete_blob(self,key):
        self._delete_cache.add(key)
        if key in self._update_cache:
            del self._update_cache[key]

    def rollback(self):
        self._delete_cache = set()
        self._update_cache = {}

class Index(object):

    """
    An index accepts key/value pairs and stores them so that they can be 
    efficiently retrieved.
    """

    def __init__(self,params,store = None):
        self._params = params
        self._store = store
        self._index = defaultdict(lambda : set())
        self._reverse_index = defaultdict(lambda : set())
        self._splitted_key = self.key.split(".")

        if store:
            self.load_from_store()

    @property
    def key(self):
        return self._params['key']

    def get_value(self,attributes):
        v = attributes
        for element in self._splitted_key:
            v = v[element]
        return v

    def save_to_store(self):
        if not self._store:
            raise AttributeError("No datastore defined!")
        data =json.dumps(self.save_to_data())
        self._store.store_blob(data,'all_keys')

    def get_all_keys(self):
        return reduce(lambda x,y:x | y,self._index.values(),set())

    def load_from_store(self):
        if not self._store:
            raise AttributeError("No datastore defined!")
        if not self._store.has_blob('all_keys'):
            return
        data = json.loads(self._store.get_blob('all_keys'))
        self.load_from_data(data)

    def save_to_data(self):
        return [(x[0],list(x[1])) for x in self._index.items()]

    def load_from_data(self,data):
        self._index = defaultdict(lambda : set())
        self._reverse_index = defaultdict(lambda : set())
        for key,values in data:
            self._index[key] = set(values)
            for value in values:
                self._reverse_index[value].add(key)

    def get_hash_for(self,value):
        if isinstance(value,dict):
            return hash(frozenset(value.items()))
        return value

    def get_keys_for(self,value):
        if callable(value):
            return reduce(lambda x,y:x | y,[v[1] for v in self._index.items() if value(v[0])])
        hash_value = self.get_hash_for(value)
        return self._index[hash_value].copy()

    #The following two operations change the value of the index

    def add_key(self,attributes,store_key):
        try:
            value = self.get_value(attributes)
        except (KeyError,IndexError):
            return
        #We remove old values
        self.remove_key(store_key)
        if isinstance(value,list):
            values = value
        else:
            values = [value]
        for value in values:
            hash_value = self.get_hash_for(value)
            self._index[hash_value].add(store_key)
            self._reverse_index[store_key].add(hash_value)

    def remove_key(self,store_key):
        if store_key in self._reverse_index:
            for v in self._reverse_index[store_key]:
                self._index[v].remove(store_key)
            del self._reverse_index[store_key]

class TransactionalIndex(Index):

    def __init__(self,params,store = None):
        super(TransactionalIndex,self).__init__(params,store = store)
        self.begin()

    def begin(self):
        self._cached_index = self.save_to_data()

    def commit(self):
        self.save_to_store()

    def rollback(self):
        self.load_from_data(self._cached_index)

class Backend(BaseBackend):

    """
    A backend stores and retrieves objects from the database.

    To Do:

    """

    CollectionStore = TransactionalStore
    Index = TransactionalIndex
    IndexStore = Store

    def __init__(self,path,autocommit = False):
        super(Backend,self).__init__()

        self._path = os.path.abspath(path)
        if not os.path.exists(path):
            os.makedirs(path)

        self.collections = {}
        self.stores = {}
        self.autocommit = autocommit
        self.indexes = defaultdict(lambda : {})
        self.index_stores = defaultdict(lambda : {})
        self.load_config()
        self.in_transaction = False
        self.begin()

    def load_config(self):
        config_file = self._path+"/config.json"
        if os.path.exists(config_file):
            with open(config_file,"rb") as config_file:
                self._config = json.loads(config_file.read())
        else:
            self._config = {
                'indexes' : {}
            }
            self.save_config()

    def save_config(self):
        config_file = self._path+"/config.json"
        with open(config_file,"wb") as config_file:
            config_file.write(json.dumps(self._config))
        
    def __del__(self):
        self.commit()

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

    def register(self,cls,parameters):
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
            raise Exception("Not in a transaction!")
        for collection,store in self.stores.items():
            store.rollback()
            indexes = self.indexes[collection]
            for index in indexes.values():
                index.rollback()
        self.in_transaction = False

    def commit(self):
        """
        Commits a transaction
        """
        for collection in self.collections:
            store = self.get_collection_store(collection)
            indexes = self.get_collection_indexes(collection)
            store.commit()
            for index in indexes.values():
                index.commit()
        self.in_transaction = False
        self.begin()

    def init_indexes(self,collection):
        if collection in self._config['indexes']:
            for index_params in self._config['indexes'][collection]:
                self.create_index(collection,index_params)

        self.create_index(collection,{'key':'pk'})

    def create_index(self,cls_or_collection,params):

        if not isinstance(params,dict):
            params = {'key' : params}

        if not isinstance(cls_or_collection,str) and not isinstance(cls_or_collection,unicode):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection

        if not 'id' in params:
            params['id'] = uuid.uuid4().hex 

        for index in self.indexes[collection].values():
            if index.key == params['key']:
                return #Index already exists

        index_store = self.get_index_store(collection,params['id'])
        index = self.Index(params,index_store)

        self.indexes[collection][params['id']] = index

        if not collection in self._config['indexes']:
            self._config['indexes'][collection] = []
        self._config['indexes'][collection].append(params)
        self.save_config()

        #Now for the hard part: We add all objects in the database to the index...
        all_objects = self.filter(collection,{})
        for obj in all_objects:
            serialized_attributes = self.serialize(obj.attributes)#optimize this!
            index.add_key(serialized_attributes,obj._store_key)

        if self.autocommit:
            self.commit()

    def get_collection_indexes(self,collection):
        return self.indexes[collection] if collection in self.indexes else {}

    def encode_attributes(self,attributes):
        return json.dumps(attributes,cls = JsonEncoder)

    def decode_attributes(self,data):
        return json.loads(data)

    def get_object(self,cls,key):
        collection = self.get_collection_for_cls(cls)
        store = self.get_collection_store(collection)
        try:
            data = self.deserialize(self.decode_attributes(store.get_blob(key)))
        except IOError:
            raise AttributeError("Object does not exist!")
        obj = self.create_instance(cls,data)
        return obj

    def save(self,obj):
        collection = self.get_collection_for_obj(obj)
        indexes = self.get_collection_indexes(collection)
        store = self.get_collection_store(collection)

        if obj.pk == None:
            obj.pk = uuid.uuid4().hex 

        serialized_attributes = self.serialize(obj.attributes)
        data = self.encode_attributes(serialized_attributes)
    
        try:
            store_key = self.get_pk_index(collection).get_keys_for(obj.pk).pop()
        except KeyError:
            store_key = uuid.uuid4().hex
    
        store.store_blob(data,store_key)

        for key,index in indexes.items():
            index.add_key(serialized_attributes,store_key)

        if self.autocommit:
            self.commit()

        return obj

    def get_pk_index(self,collection):
        return [idx for idx in self.indexes[collection].values() if idx.key == 'pk'][0]

    def delete(self,obj):
        
        collection = self.get_collection_for_obj(obj)
        store = self.get_collection_store(collection)
        indexes = self.get_collection_indexes(collection)
        
        store_keys = self.get_pk_index(collection).get_keys_for(obj.pk)
        
        for store_key in store_keys:
            try:
                store.delete_blob(store_key)
            except IOError:
                pass
            for index in indexes.values():
                index.remove_key(store_key)

        if self.autocommit:
            self.commit()

    def get(self,cls,query):
        objects = self.filter(cls,query,limit = 1)
        if len(objects) == 0 or len(objects) > 1:
            raise AttributeError
        return objects[0]
        
    def filter(self,cls_or_collection,query,sort_by = None,limit = None,offset = None):

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
        compiled_query = self.compile_query(query)

        unindexed_queries = []
        indexed_queries = []

        indexes_by_key = dict([(idx.key,idx) for idx in indexes.values()])

        for key,accessor,value in compiled_query:
            if key in indexes_by_key:
                indexed_queries.append([indexes_by_key[key],value])
            else:
                unindexed_queries.append([accessor,value])

        if indexed_queries:
            keys = None
            for index,value in indexed_queries:
                if not keys:
                    keys = index.get_keys_for(value)
                else:
                    keys &= index.get_keys_for(value)
        else:
            keys = self.get_pk_index(collection).get_all_keys()

        for accessor,value in unindexed_queries:
            keys_to_remove = set()
            for key in keys:
                try:
                    attributes = self.decode_attributes(store.get_blob(key))
                except IOError:
                    raise
                    raise Exception("Index is corrupt!")
                try:
                    if callable(value):
                        if not value(accessor(attributes)):
                            keys_to_remove.add(key)
                    else:
                        accessed_value = accessor(attributes)
                        if isinstance(accessed_value,list):
                            if value not in accessed_value: 
                                keys_to_remove.add(key)
                        elif accessed_value != value:
                            keys_to_remove.add(key) 
                except (KeyError,IndexError):
                    keys_to_remove.add(key)
            keys -= keys_to_remove

        return QuerySet(self,store,cls,keys)

