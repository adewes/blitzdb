import os
import os.path

import gzip
import hashlib
import datetime
import uuid
import copy

from collections import defaultdict
from blitzdb.backends.file.serializers import PickleSerializer as Serializer

"""
"""


class Store(object):

    """
    This class stores binary data in files.
    """

    def __init__(self, properties):
        self._properties = properties

        if 'path' not in properties:
            raise AttributeError("You must specify a path when creating a Store!")

        if not os.path.exists(properties['path']):
            os.makedirs(properties['path'])

    def _get_path_for_key(self, key):
        return os.path.join(self._properties['path'], key)

    def store_blob(self, blob, key):
        with open(self._get_path_for_key(key), "wb") as output_file:
            output_file.write(blob)
        return key

    def delete_blob(self, key):
        filepath = self._get_path_for_key(key)
        if os.path.exists(filepath):
            os.unlink(filepath)

    def get_blob(self, key):
        try:
            with open(self._get_path_for_key(key), "rb") as input_file:
                return input_file.read()
        except IOError:
            raise KeyError("Key {0} not found!".format(key))

    def has_blob(self, key):
        if os.path.exists(self._get_path_for_key(key)):
            return True
        return False

    def begin(self):
        pass

    def rollback(self):
        pass

    def commit(self):
        pass


class TransactionalStore(Store):

    """
    This class adds transaction support to the Store class.
    """

    def __init__(self, properties):
        super(TransactionalStore, self).__init__(properties)
        self._enabled = True
        self.begin()

    def begin(self):
        self._delete_cache = set()
        self._update_cache = {}

    def commit(self):
        try:
            self._enabled = False
            for store_key in self._delete_cache:
                if super(TransactionalStore, self).has_blob(store_key):
                    super(TransactionalStore, self).delete_blob(store_key)
            for store_key, blob in self._update_cache.items():
                super(TransactionalStore, self).store_blob(blob, store_key)
        finally:
            self._enabled = True

    def has_blob(self, key):
        if not self._enabled:
            return super(TransactionalStore, self).has_blob(key)
        if key in self._delete_cache:
            return False
        if key in self._update_cache:
            return True
        return super(TransactionalStore, self).has_blob(key)

    def get_blob(self, key):
        if not self._enabled:
            return super(TransactionalStore, self).get_blob(key)
        if key in self._update_cache:
            return self._update_cache[key]
        return super(TransactionalStore, self).get_blob(key)

    def store_blob(self, blob, key, *args, **kwargs):
        if not self._enabled:
            return super(TransactionalStore, self).store_blob(blob, key, *args, **kwargs)
        if key in self._delete_cache:
            self._delete_cache.remove(key)
        self._update_cache[key] = copy.copy(blob)
        return key

    def delete_blob(self, key, *args, **kwargs):
        if not self._enabled:
            return super(TransactionalStore, self).delete_blob(key, *args, **kwargs)
        if not self.has_blob(key):
            raise KeyError("Key %s not found!" % key)
        self._delete_cache.add(key)
        if key in self._update_cache:
            del self._update_cache[key]

    def rollback(self):
        self._delete_cache = set()
        self._update_cache = {}
