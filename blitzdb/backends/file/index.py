from collections import defaultdict
import json
from blitzdb.backends.file.utils import JsonEncoder

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
            self.loaded = self.load_from_store()

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
            return False
        data = json.loads(self._store.get_blob('all_keys'))
        self.load_from_data(data)
        return True

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
