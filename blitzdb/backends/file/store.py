import os
import os.path

import gzip
import hashlib
import datetime
import uuid
import copy

from collections import defaultdict
from serializers import PickleSerializer as Serializer

"""
"""


class Store(object):

    """
    This class stores binary data in files.
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
        try:
            with open(self._properties['path']+"/"+key,"r") as input_file:
                return input_file.read()
        except IOError:
            raise KeyError("Key %s not found!" % key)

    def has_blob(self,key):
        if os.path.exists(self._properties['path']+"/"+key):
            return True
        return False

    def begin(self):
        pass

    def rollback(self):
        pass

    def commit(self):
        pass

class CompressedStore(Store):

    """
    Increase compression efficiency by storing multiple blobs in a single file object.

    WARNING: This makes uncached writes and deletes significantly slower!
    """

    def __init__(self,properties,max_file_size = 1024*128):
        self._properties = properties
        self._max_file_size = max_file_size
        if not 'path' in properties:
            raise AttributeError("You must specify a path when creating a Store!")

        if not os.path.exists(properties['path']):
            os.makedirs(properties['path'])
        self.load_map_file()

    def _map_filename(self):
        return self._properties['path']+"/map.json"

    def load_map_file(self):
        self._map = {}
        self._reverse_map = defaultdict(lambda : [])
        self._blob_sizes = defaultdict(lambda : 0)
        self._current_blob_key = None

        map_filename = self._map_filename()
        if os.path.exists(map_filename):
            with open(map_filename,"rb") as map_file:
                self._map = Serializer.deserialize(map_file.read())

            for store_key,(blob_key,start,stop) in self._map.items():
                if not store_key in self._reverse_map[blob_key]:
                    self._reverse_map[blob_key].append(store_key)
                self._blob_sizes[blob_key]+=stop-start
            smallest_size = None
            for blob_key,size in self._blob_sizes.items():
                if not smallest_size or size < smallest_size:
                    self._current_blob_key = blob_key
                    smallest_size = size

    def get_blob_key_for_write(self,force_new = False):
        if not self._current_blob_key or force_new or len(self._reverse_map[self._current_blob_key]) > 10:
            self._current_blob_key = uuid.uuid4().hex
            self._blob_sizes[self._current_blob_key] = 0
            self._reverse_map[self._current_blob_key] = []
        return self._current_blob_key

    def write_map_file(self):
        map_filename = self._map_filename()
        with open(map_filename,"wb") as map_file:
            map_file.write(Serializer.serialize(self._map))

    def _get_path_for_key(self,blob_key):
        return self._properties['path']+'/'+blob_key

    def store_blob(self,blob,key,write_map = True,delete_old = True):

        if key in self._map and delete_old:
            self.delete_blob(key)

        blob_key = self.get_blob_key_for_write()

        start = self._blob_sizes[blob_key]
        stop = start+len(blob)

        with open(self._get_path_for_key(blob_key),"a") as output_file:
            output_file.write(blob)

        self._map[key] = (blob_key,start,stop)
        self._reverse_map[blob_key].append(key)
        self._blob_sizes[blob_key]+=len(blob)

        if write_map:
            self.write_map_file()

        return key

    def delete_blob(self,key):
        blob_key,start,stop = self._map[key]
        store_keys = self._reverse_map[blob_key]

        current_blob_key = self.get_blob_key_for_write()
        if current_blob_key == blob_key:
            self.get_blob_key_for_write(force_new = True) 

        store_keys.remove(key)
        del self._map[key]

        for store_key in store_keys:
            self.store_blob(self.get_blob(store_key),store_key,write_map = False,delete_old = False)

        filepath = self._get_path_for_key(blob_key)

        if os.path.exists(filepath):
            os.unlink(filepath)

        del self._reverse_map[blob_key]
        del self._blob_sizes[blob_key]
    
        self.write_map_file()

    def get_blob(self,key):
        blob_key,start,stop = self._map[key]
        filepath = self._get_path_for_key(blob_key)
        with open(filepath,"r") as input_file:
            input_file.seek(start)
            content = input_file.read(stop-start)
            return content

    def has_blob(self,key):
        if key in self._map:
            return True
        return False

class TransactionalStore(Store):

    """
    This class adds transaction support to the Store class.
    """

    def __init__(self,properties):
        super(TransactionalStore,self).__init__(properties)
        self._enabled = True
        self.begin()

    def begin(self):
        self._delete_cache = set()
        self._update_cache = {}

    def commit(self):
        try:
            self._enabled = False
            for store_key in self._delete_cache:
                if super(TransactionalStore,self).has_blob(store_key):
                    super(TransactionalStore,self).delete_blob(store_key)
            for store_key,blob in self._update_cache.items():
                super(TransactionalStore,self).store_blob(blob,store_key)
        finally:
            self._enabled = True

    def has_blob(self,key):
        if not self._enabled:
            return super(TransactionalStore,self).has_blob(key)
        if key in self._delete_cache:
            return False
        if key in self._update_cache:
            return True
        return super(TransactionalStore,self).has_blob(key)

    def get_blob(self,key):
        if not self._enabled:
            return super(TransactionalStore,self).get_blob(key)
        if key in self._update_cache:
            return self._update_cache[key]
        return super(TransactionalStore,self).get_blob(key)

    def store_blob(self,blob,key,*args,**kwargs):
        if not self._enabled:
            return super(TransactionalStore,self).store_blob(blob,key,*args,**kwargs)
        if key in self._delete_cache:
            self._delete_cache.remove(key)
        self._update_cache[key] = copy.copy(blob)
        return key

    def delete_blob(self,key,*args,**kwargs):
        if not self._enabled:
            return super(TransactionalStore,self).delete_blob(key,*args,**kwargs)
        if not self.has_blob(key):
            raise KeyError("Key %s not found!" % key)
        self._delete_cache.add(key)
        if key in self._update_cache:
            del self._update_cache[key]

    def rollback(self):
        self._delete_cache = set()
        self._update_cache = {}

class TransactionalCompressedStore(TransactionalStore,CompressedStore):

    """
    Exactly what the name says.
    """
