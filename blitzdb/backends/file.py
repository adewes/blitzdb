from blitzdb.backends.base import Backend as BaseBackend

import os
import os.path

import json
import hashlib
import datetime
import uuid

from collections import defaultdict

class JsonEncoder(json.JSONEncoder):
    
    def default(self, obj):
        if isinstance(obj,set):
            return list(obj)
        elif isinstance(obj,datetime.datetime):
            return obj.ctime()
        return json.JSONEncoder.default(self, obj)

class Storage(object):

    """
    A storage object stores and retrieves blobs.
    """

    def __init__(self,properties):
        self._properties = properties

    def store_blob(self,blob,key):
        with open(self._properties['path']+"/"+key+".json","w") as output_file:
            output_file.write(blob)
        return key

    def _get_path_for_key(self,key):
        return self._properties['path']+'/'+key+'.json'

    def delete_blob(self,key):
        filepath = self._get_path_for_key(key)
        if os.path.exists(filepath):
            os.unlink(filepath)

    def get_blob(self,key):
        with open(self._properties['path']+"/"+key+".json","r") as input_file:
            return input_file.read()

    def has_blob(self,key):
        pass

class Index(object):

    """
    An index accepts key/value pairs and stores them so that they can be 
    efficiently retrieved.
    """

    def __init__(self,key):
        self._key = key
        self._index = defaultdict(lambda : set())
        self._reverse_index = defaultdict(lambda : set())
        self._splitted_key = key.split(".")

    @property
    def key(self):
        return self._key

    def get_value(self,attributes):
        v = attributes
        for element in self._splitted_key:
            v = v[element]
        return v

    def dump(self):
        return [(x[0],list(x[1])) for x in self._index.items()]

    def get_all_keys(self):
        return reduce(lambda x,y:x | y,self._index.values(),set())

    def load(self,data):
        self._index = defaultdict(lambda : set())
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

    def add_key(self,attributes,storage_key):
        try:
            value = self.get_value(attributes)
        except (KeyError,IndexError):
            return
        #We remove old values
        self.remove_key(storage_key)
        if isinstance(value,list):
            values = value
        else:
            values = [value]
        for value in values:
            hash_value = self.get_hash_for(value)
            self._index[hash_value].add(storage_key)
            self._reverse_index[storage_key].add(hash_value)

    def remove_key(self,storage_key):
        if storage_key in self._reverse_index:
            for v in self._reverse_index[storage_key]:
                self._index[v].remove(storage_key)
            del self._reverse_index[storage_key]

class QuerySet(object):

    """
    A queryset accepts a list of storage keys and returns the corresponding objects on demand.
    """

    def __init__(self,backend,storage,cls,keys):
        self.cls = cls
        self.storage = storage
        self.keys = list(keys)
        self.backend = backend
        self.objects = {}

    def __getitem__(self,i):
        key = self.keys[i]
        if not key in self.objects:
            self.objects[key] = self.backend.get_object(self.cls,key)
            self.objects[key]._storage_key = key
        return self.objects[key]

    def delete(self):
        for i in range(0,len(self.keys)):
            try:
                obj = self[i]
                self.backend.delete(obj)
            except AttributeError:
                pass
        self.keys = []
        self.objects = {}

    def __len__(self):
        return len(self.keys)

class Backend(BaseBackend):

    """
    A backend stores and retrieves objects from the database.
    """

    def __init__(self,path):
        super(Backend,self).__init__()

        self._path = os.path.abspath(path)
        if not os.path.exists(path):
            os.makedirs(path)

        self.storages = {}
        self.indexes = defaultdict(lambda : {})

    @property
    def path(self):
        return self._path

    def get_storage_for_collection(self,collection):
        if collection in self.storages:
            return self.storages[collection]

        storage_path = self.path+"/"+collection+"/objects"
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        self.storages[collection] = Storage({'path':storage_path})
        return self.storages[collection]

    def register(self,cls,parameters):
        super(Backend,self).register(cls,parameters)
        self.load_indexes(self.get_collection_name_for_cls(cls))

    def rebuild_indexes(self):
        for collection,storage in self.storages.items():
            indexes = self.indexes[collection]

    def __del__(self):
        self.save_indexes()

    def save_indexes(self):
        for collection,indexes in self.indexes.items():
            indexes_path = self.path+"/"+collection+"/indexes"
            if not os.path.exists(indexes_path):
                os.makedirs(indexes_path)
            for index_name,index in indexes.items():
                json_data = json.dumps(index.dump())
                with open(indexes_path+"/"+index_name,"wb") as index_file:
                    index_file.write(json_data)

    def load_indexes(self,collection):
        indexes_path = self.path+"/"+collection+"/indexes"
        if not os.path.exists(indexes_path):
            os.makedirs(indexes_path)
        filenames = os.listdir(indexes_path)
        for filename in filenames:
            try:
                with open(indexes_path+"/"+filename,"rb") as index_file:
                    index_data = json.loads(index_file.read())
                    index = Index(filename)
                    index.load(index_data)
                    self.indexes[collection][filename] = index
            except:
                print "Failed to load index: %s" % filename

        if not 'pk' in self.indexes[collection]:
            index = Index('pk')
            self.indexes[collection]['pk'] = index

    def create_index(self,cls,key):
        collection = self.get_collection_name_for_cls(cls)
        if key in self.indexes[collection]:
            return
        self.indexes[collection][key] = Index(key)
        all_objects = self.filter(cls,{})
        for obj in all_objects:
            serialized_attributes = self.serialize(obj.attributes)
            self.indexes[collection][key].add_key(serialized_attributes,obj._storage_key)

    def get_indexes_for_collection(self,collection):
        return self.indexes[collection] if collection in self.indexes else {}

    def get_object(self,cls,key):
        collection = self.get_collection_name_for_cls(cls)
        storage = self.get_storage_for_collection(collection)
        try:
            data = self.deserialize(self.decode_attributes(storage.get_blob(key)))
        except IOError:
            raise AttributeError("Object does not exist!")
        obj = self.create_instance(cls,data)
        return obj

    def save(self,obj):
        collection = self.get_collection_name_for_obj(obj)
        indexes = self.get_indexes_for_collection(collection)
        storage = self.get_storage_for_collection(collection)

        if obj.pk == None:
            obj.pk = uuid.uuid4().hex 

        serialized_attributes = self.serialize(obj.attributes)
        data = self.encode_attributes(serialized_attributes)
    
        try:
            storage_key = indexes['pk'].get_keys_for(obj.pk).pop()
        except KeyError:
            storage_key = uuid.uuid4().hex
    
        storage.store_blob(data,storage_key)

        for key,index in indexes.items():
            index.add_key(serialized_attributes,storage_key)

        return obj

    def encode_attributes(self,attributes):
        return json.dumps(attributes,cls = JsonEncoder)

    def decode_attributes(self,data):
        return json.loads(data)

    def delete(self,obj):
        
        collection = self.get_collection_name_for_obj(obj)
        storage = self.get_storage_for_collection(collection)
        indexes = self.get_indexes_for_collection(collection)
        
        storage_keys = indexes['pk'].get_keys_for(obj.pk)
        
        for storage_key in storage_keys:
            try:
                storage.delete_blob(storage_key)
            except IOError:
                pass
            for index in indexes.values():
                index.remove_key(storage_key)

        obj.pk = None
                
    def get(self,cls,query):
        objects = self.filter(cls,query,limit = 1)
        if len(objects) == 0 or len(objects) > 1:
            raise AttributeError
        return objects[0]
        
    def filter(self,cls,query,sort_by = None,limit = None,offset = None):

        if not isinstance(query,dict):
            raise AttributeError("Query parameters must be dict!")

        collection = self.get_collection_name_for_cls(cls)
        storage = self.get_storage_for_collection(collection)
        indexes = self.get_indexes_for_collection(collection)
        compiled_query = self.compile_query(query)
        unindexed_queries = []
        indexed_queries = []

        for key,accessor,value in compiled_query:
            if key in indexes:
                indexed_queries.append([indexes[key],value])
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
            keys = indexes['pk'].get_all_keys()


        for accessor,value in unindexed_queries:
            keys_to_remove = set()
            for key in keys:
                try:
                    attributes = self.decode_attributes(storage.get_blob(key))
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

        return QuerySet(self,storage,cls,keys)

