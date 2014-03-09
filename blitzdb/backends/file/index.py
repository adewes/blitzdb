from collections import defaultdict
import copy
from blitzdb.backends.file.utils import JsonEncoder
from blitzdb.backends.base import NotInTransaction
from serializers import PickleSerializer as Serializer
import time

class Index(object):

    """
    An index accepts key/value pairs and stores them so that they can be 
    efficiently retrieved.
    """

    def __init__(self,params,store = None):
        self._params = params
        self._store = store
        self._splitted_key = self.key.split(".")
        self.clear()
        if store:
            self.ephemeral = False
            self.loaded = self.load_from_store()
        else:
            self.ephemeral = True

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
        return self._index

    def load_from_store(self):
        if not self._store:
            raise AttributeError("No datastore defined!")
        if not self._store.has_blob('all_keys'):
            return False
        data = Serializer.deserialize(self._store.get_blob('all_keys'))
        self.load_from_data(data)
        return True

    def save_to_data(self,in_place = False):
        if in_place:
            return self._index.items()
        return [(key,values[:]) for key,values in self._index.items()]

    def load_from_data(self,data):
        self._index = defaultdict(list,data)
        self._reverse_index = defaultdict(list)
        [self._reverse_index[value].append(key) for key,values in self._index.items() for value in values]

    def get_hash_for(self,value):
        if isinstance(value,dict):
            return hash(frozenset([self.get_hash_for(x) for x in value.items()]))
        elif isinstance(value,list) or isinstance(value,tuple):
            return hash(tuple([self.get_hash_for(x) for x in value]))
        return value

    def get_keys_for(self,value):
        if callable(value):
            return value(self)
        hash_value = self.get_hash_for(value)
        return self._index[hash_value][:]

    #The following two operations change the value of the index

    def add_key(self,attributes,store_key):

        def add_hashed_value(value):
            if not store_key in self._index[hash_value]:
                self._index[hash_value].append(store_key)
            if not hash_value in self._reverse_index[store_key]:
                self._reverse_index[store_key].append(hash_value)

        try:
            value = self.get_value(attributes)
        except (KeyError,IndexError):
            return
        #We remove old values
        self.remove_key(store_key)
        if isinstance(value,list) or isinstance(value,tuple):
            values = value
            #We add an extra hash value for the list itself (this allows for querying the whole list)
            hash_value = self.get_hash_for(value)
            add_hashed_value(hash_value)
        else:
            values = [value]

        for value in values:
            hash_value = self.get_hash_for(value)
            add_hashed_value(hash_value)

    def remove_key(self,store_key):
        if store_key in self._reverse_index:
            for v in self._reverse_index[store_key]:
                self._index[v].remove(store_key)
            del self._reverse_index[store_key]


class TransactionalIndex(Index):

    """
    This class adds transaction support to the Index class.
    """

    def __init__(self,params,store = None):
        super(TransactionalIndex,self).__init__(params,store = store)

    def begin(self):
        self._cached_index = self.save_to_data()

    def commit(self):
        if not self.ephemeral:
            self.save_to_store()

    def rollback(self):
        if not hasattr(self,'_cached_index'):
            raise NotInTransaction
        self.load_from_data(self._cached_index)
        delattr(self,'_cached_index')

