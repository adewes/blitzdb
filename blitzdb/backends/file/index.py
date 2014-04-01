from collections import defaultdict
import copy
from blitzdb.backends.file.utils import JsonEncoder
from blitzdb.backends.base import NotInTransaction
from blitzdb.backends.file.serializers import PickleSerializer as Serializer
import time


class Index(object):

    """
    An index accepts key/value pairs and stores them so that they can be 
    efficiently retrieved.
    """

    def __init__(self,params,serializer,deserializer,store = None):
        self._params = params
        self._store = store
        self._serializer = serializer
        self._deserializer = deserializer
        self._splitted_key = self.key.split(".")
        self.clear()
        if store:
            self.ephemeral = False
            self.loaded = self.load_from_store()
        else:
            self.ephemeral = True
            self.loaded = False

    def clear(self):
        self._index = defaultdict(lambda : [])
        self._reverse_index = defaultdict(lambda : [])

    @property
    def key(self):
        return self._params['key']

    def get_value(self,attributes):
        v = attributes
        for elem in self._splitted_key:
            if isinstance(v,list):
                v = v[int(elem)]
            else:
                v = v[elem]
        return v

    def save_to_store(self):
        if not self._store:
            raise AttributeError("No datastore defined!")
        saved_data = self.save_to_data(in_place = True)
        data = Serializer.serialize(saved_data)
        self._store.store_blob(data,'all_keys')
        
    def get_all_keys(self):
        all_keys = []
        [all_keys.extend(l) for l in self._index.values()]
        return all_keys

    def get_index(self):
        return copy.deepcopy(self._index)

    def load_from_store(self):
        if not self._store:
            raise AttributeError("No datastore defined!")
        if not self._store.has_blob('all_keys'):
            return False
        data = Serializer.deserialize(self._store.get_blob('all_keys'))
        self.load_from_data(data)
        return True

    def sort_keys(self,keys,order = 1):
        #to do: check that all reverse index values are unambiguous
        missing_keys = [key for key in keys if not len(self._reverse_index[key])]
        keys_and_values = [(key,self._reverse_index[key][0]) for key in keys if not key in missing_keys]
        sorted_keys = [kv[0] for kv in sorted(keys_and_values,key = lambda x: x[1],reverse = True if order < 0 else False)]
        if order > 0:
            return missing_keys + sorted_keys
        else:
            return sorted_keys + missing_keys

    def save_to_data(self,in_place = False):
        if in_place:
            return list(self._index.items())
        return [(key,values[:]) for key,values in self._index.items()]

    def load_from_data(self,data):
        self._index = defaultdict(list,data)
        self._reverse_index = defaultdict(list)
        [self._reverse_index[value].append(key) for key,values in self._index.items() for value in values]

    def get_hash_for(self,value):
        serialized_value = self._serializer(value)
        if isinstance(serialized_value,dict):
            return hash(frozenset([self.get_hash_for(x) for x in serialized_value.items()]))
        elif isinstance(serialized_value,list) or isinstance(serialized_value,tuple):
            return hash(tuple([self.get_hash_for(x) for x in serialized_value]))
        return value

    def get_keys_for(self,value):
        if callable(value):
            return value(self)
        hash_value = self.get_hash_for(value)
        return self._index[hash_value][:]

    #The following two operations change the value of the index

    def add_hashed_value(self,hash_value,store_key):
        if not store_key in self._index[hash_value]:
            self._index[hash_value].append(store_key)
        if not hash_value in self._reverse_index[store_key]:
            self._reverse_index[store_key].append(hash_value)

    def add_key(self,attributes,store_key):

        try:
            value = self.get_value(attributes)
        except (KeyError,IndexError):
            return

        #We remove old values
        self.remove_key(store_key)
        if isinstance(value,list) or isinstance(value,tuple):
            #We add an extra hash value for the list itself (this allows for querying the whole list)
            values = value
            hash_value = self.get_hash_for(value)
            self.add_hashed_value(hash_value,store_key)
        else:
            values = [value]

        for value in values:
            hash_value = self.get_hash_for(value)
            self.add_hashed_value(hash_value,store_key)

    def remove_key(self,store_key):
        if store_key in self._reverse_index:
            for v in self._reverse_index[store_key]:
                self._index[v].remove(store_key)
            del self._reverse_index[store_key]


class TransactionalIndex(Index):

    """
    This class adds transaction support to the Index class.
    """

    def __init__(self,*args,**kwargs):
        super(TransactionalIndex,self).__init__(*args,**kwargs)
        self._in_transaction = False
        self._init_cache()

    def _init_cache(self):
        self._add_cache = defaultdict(lambda : [])
        self._reverse_add_cache = defaultdict(lambda : [])
        self._remove_cache = {}

    def begin(self):
        self.commit()

    def commit(self):

        if not self._add_cache and not self._remove_cache:
            return

        for store_key,hash_values in self._add_cache.items():
            for hash_value in hash_values:
                super(TransactionalIndex,self).add_hashed_value(hash_value,store_key)            
        for store_key in self._remove_cache:
            super(TransactionalIndex,self).remove_key(store_key)
        if not self.ephemeral:
            self.save_to_store()
    
        self._init_cache()
        self._in_transaction = True

    def rollback(self):
        if not self._in_transaction:
            raise NotInTransaction
        self._init_cache()
        self._in_transaction = False

    def add_hashed_value(self,hash_value,store_key):
        if not hash_value in self._add_cache[store_key]:
            self._add_cache[store_key].append(hash_value)
        if not store_key in self._reverse_add_cache[hash_value]:
            self._reverse_add_cache[hash_value].append(store_key)
        if store_key in self._remove_cache:
            del self._remove_cache[store_key]

    def remove_key(self,store_key):
        self._remove_cache[store_key] = True
        if store_key in self._add_cache:
            for hash_value in self._add_cache[store_key]:
                self._reverse_add_cache[hash_value].remove(store_key)
            del self._add_cache[store_key]

    def get_keys_for(self,value,include_uncommitted = False):
        if not include_uncommitted:
            return super(TransactionalIndex,self).get_keys_for(value)
        else:
            keys = super(TransactionalIndex,self).get_keys_for(value)
            hash_value = self.get_hash_for(value)
            keys+=self._reverse_add_cache[hash_value]
            return keys
