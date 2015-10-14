import os
import os.path
import uuid

from collections import defaultdict

import blitzdb

from blitzdb.document import Document
from blitzdb.backends.base import (
    Backend as BaseBackend,
    NotInTransaction,
)
from blitzdb.backends.file.index import (
    Index,
    TransactionalIndex,
)
from blitzdb.backends.file.queries import compile_query
from blitzdb.backends.file.queryset import QuerySet
from blitzdb.backends.file.serializers import (
    JsonSerializer,
    PickleSerializer,
)
from blitzdb.backends.file.store import (
    Store,
    TransactionalStore,
)
from blitzdb.helpers import get_value,set_value,delete_value

import six


store_classes = {
    'transactional': TransactionalStore,
    'basic': Store,
}

index_classes = {
    'transactional': TransactionalIndex,
    'basic': Index
}

serializer_classes = {
    'pickle': PickleSerializer,
    'json': JsonSerializer,
}

# will only be available if cjson is installed
try:
    from blitzdb.backends.file.serializers import CJsonSerializer
    serializer_classes['cjson'] = CJsonSerializer
except ImportError:
    pass


class DatabaseIndexError(BaseException):

    """Gets raised when the index of the database is corrupted.

    Ideally this should never happen. To recover from this error, you can call
    the `rebuild_index` function of the file backend with the affected
    collection and key as parameters.

    """


class Backend(BaseBackend):

    """A file-based database backend.

    Uses flat files to store objects on the hard disk and file-based indexes to
    optimize querying.

    :param path: The path to the database. If non-existant it will be created
    :param config:
        The configuration dictionary. If not specified, Blitz will try to load
        it from disk.  If this fails, the default configuration will be used
        instead.

    .. warning::
        It might seem tempting to use the `autocommit` config and not having to
        worry about calling `commit` by hand. Please be advised that this can
        incur a significant overhead in write time since a `commit` will
        trigger a complete rewrite of all indexes to disk.

    """

    # the default configuration values.
    default_config = {
        'indexes': {},
        'store_class': 'transactional',
        'index_class': 'transactional',
        'index_store_class': 'basic',
        'serializer_class': 'json',
        'autocommit': False,
    }

    config_defaults = {}

    def __init__(self, path, config=None, overwrite_config=False, **kwargs):

        self._path = os.path.abspath(path)
        if not os.path.exists(path):
            os.makedirs(path)

        self.collections = {}
        self.stores = {}
        self.in_transaction = False
        self.indexes = defaultdict(lambda: {})
        self.index_stores = defaultdict(lambda: {})
        self.load_config(config, overwrite_config)
        self._auto_transaction = False
        self.begin()

        super(Backend, self).__init__(**kwargs)

    @property
    def autocommit(self):
        return 'autocommit' in self.config and self.config['autocommit']

    @autocommit.setter
    def autocommit(self, value):
        if value not in (True, False):
            raise TypeError('Value must be boolean!')
        self.config['autocommit'] = value

    def begin(self):
        """Start a new transaction."""
        if self.in_transaction:  # we're already in a transaction...
            if self._auto_transaction:
                self._auto_transaction = False
                return
            self.commit()
        self.in_transaction = True
        for collection, store in self.stores.items():
            store.begin()
            indexes = self.indexes[collection]
            for index in indexes.values():
                index.begin()

    @property
    def StoreClass(self):
        return store_classes[self.config['store_class']]

    @property
    def IndexClass(self):
        return index_classes[self.config['index_class']]

    @property
    def IndexStoreClass(self):
        return store_classes[self.config['index_store_class']]

    @property
    def SerializerClass(self):
        return serializer_classes[self.config['serializer_class']]

    def rollback(self, transaction = None):
        """Roll back a transaction."""
        if not self.in_transaction:
            raise NotInTransaction
        for collection, store in self.stores.items():
            store.rollback()
            indexes = self.indexes[collection]
            indexes_to_rebuild = []
            for key, index in indexes.items():
                try:
                    index.rollback()
                except NotInTransaction:
                    # this index is "dirty" and needs to be rebuilt
                    # (probably it has been created within a transaction)
                    indexes_to_rebuild.append(key)
            if indexes_to_rebuild:
                self.rebuild_indexes(collection, indexes_to_rebuild)
        self.in_transaction = False

    def commit(self,transaction = None):
        """Commit all pending transactions to the database.

        .. admonition:: Warning

            This operation can be **expensive** in runtime if a large number of
            documents (>100.000) is contained in the database, since it will
            cause all database indexes to be written to disk.

        """
        for collection in self.collections:
            store = self.get_collection_store(collection)
            store.commit()
            indexes = self.get_collection_indexes(collection)
            for index in indexes.values():
                index.commit()
        self.in_transaction = False
        self.begin()

    def rebuild_index(self, collection, key):
        """Rebuild a given index using the objects stored in the database.

        :param collection:
            The name of the collection for which to rebuild the index
        :param key: The key of the index to be rebuilt
        """
        return self.rebuild_indexes(collection, [key])

    def create_index(self, cls_or_collection,
                     params=None, fields=None, ephemeral=False, unique=False):
        """Create new index on the given collection/class with given parameters.

        :param cls_or_collection:
            The name of the collection or the class for which to create an
            index
        :param params: The parameters of the index
        :param ephemeral: Whether to create a persistent or an ephemeral index
        :param unique: Whether the indexed field(s) must be unique

        `params` expects either a dictionary of parameters or a string value.
        In the latter case, it will interpret the string as the name of the key
        for which an index is to be created.

        If `ephemeral = True`, the index will be created only in memory and
        will not be written to disk when :py:meth:`.commit` is called. This is
        useful for optimizing query performance.

        ..notice::

           By default, BlitzDB will create ephemeral indexes for all keys over
           which you perform queries, so after you've run a query on a given
           key for the first time, the second run will usually be much faster.

        **Specifying keys**

        Keys can be specified just like in MongoDB, using a dot ('.') to
        specify nested keys.

        .. code-block:: python

           actor = Actor({'name' : 'Charlie Chaplin',
            'foo' : {'value' : 'bar'}})

        If you want to create an index on `actor['foo']['value']` , you can
        just say

        .. code-block:: python

           backend.create_index(Actor,'foo.value')

        .. warning::

            Transcendental indexes (i.e. indexes transcending the boundaries of
            referenced objects) are currently not supported by Blitz, which
            means you can't create an index on an attribute value of a document
            that is embedded in another document.

        """
        if params:
            return self.create_indexes(cls_or_collection, [params],
                                       ephemeral=ephemeral, unique=unique)
        elif fields:
            params = []
            if len(fields.items()) > 1:
                raise ValueError("File backend currently does not support multi-key indexes, sorry :/")
            return self.create_indexes(cls_or_collection, [{'key': list(fields.keys())[0]}],
                                       ephemeral=ephemeral, unique=unique)
        else:
            raise AttributeError('You must either specify params or fields!')

    def get_pk_index(self, collection):
        """Return the primary key index for a given collection.

        :param collection: the collection for which to return the primary index

        :returns: the primary key index of the given collection

        """
        cls = self.collections[collection]

        if not cls.get_pk_name() in self.indexes[collection]:
            self.create_index(cls.get_pk_name(), collection)
        return self.indexes[collection][cls.get_pk_name()]

    def load_config(self, config=None, overwrite_config=False):
        config_file = os.path.join(self._path, "config.json")
        if os.path.exists(config_file):
            with open(config_file, 'rb') as config_file:
                # configuration is always stored in JSON format
                self._config = JsonSerializer.deserialize(config_file.read())
        else:
            if config:
                self._config = config.copy()
            else:
                self._config = {}
        if overwrite_config and config:
            self._config.update(config)

        for key, value in self.default_config.items():
            if key not in self._config:
                self._config[key] = value
        if 'version' not in self._config:
            self._config['version'] = blitzdb.__version__
        self.save_config()

    def save_config(self):
        config_file = os.path.join(self._path, 'config.json')
        with open(config_file, 'wb') as config_file:
            config_file.write(JsonSerializer.serialize(self._config))

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = config
        self.save_config()

    @property
    def path(self):
        return self._path

    def get_collection_store(self, collection):
        if collection not in self.stores:
            self.stores[collection] = self.StoreClass({
                'path': os.path.join(self.path, collection, "objects"),
                'version': self._config['version']
            })
        return self.stores[collection]

    def get_index_store(self, collection, store_key):
        if store_key not in self.index_stores[collection]:
            self.index_stores[collection][store_key] = self.IndexStoreClass({
                'path': os.path.join(self.path, collection, "indexes",
                                     store_key),
                'version': self._config['version']})
        return self.index_stores[collection][store_key]

    def register(self, cls, parameters=None):
        if super(Backend, self).register(cls, parameters):
            self.init_indexes(self.get_collection_for_cls(cls))

    def get_storage_key_for(self, obj):
        collection = self.get_collection_for_obj(obj)
        pk_index = self.get_pk_index(collection)
        try:
            return pk_index.get_keys_for(obj.pk)[0]
        except (KeyError, IndexError):
            raise obj.DoesNotExist

    def init_indexes(self, collection):
        cls = self.collections[collection]
        if collection in self._config['indexes']:
            # If not pk index is present, we create one on the fly...
            if not [idx for idx in self._config['indexes'][collection].values()
                    if idx['key'] == cls.get_pk_name()]:
                self.create_index(collection, {'key': cls.get_pk_name()})

            # We sort the indexes such that pk is always created first...
            for index_params in sorted(
                    self._config['indexes'][collection].values(),
                    key=lambda x: 0 if x['key'] == cls.get_pk_name() else 1):
                self.create_index(collection, index_params)
        else:
            # If no indexes are given, we just create a primary key index...
            self.create_index(collection, {'key': cls.get_pk_name()})

    def rebuild_indexes(self, collection, keys):
        if not keys:
            return
        all_objects = self.filter(collection, {})
        for key in keys:
            index = self.indexes[collection][key]
            index.clear()
        for key in keys:
            index = self.indexes[collection][key]
            for obj in all_objects:
                index.add_key(self.serialize(obj.attributes), obj._store_key)
            index.commit()

    def create_indexes(self, cls_or_collection, params_list, ephemeral=False, unique=False):
        indexes = []
        keys = []

        if not params_list:
            return

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection

        for params in params_list:
            if not isinstance(params, dict):
                params = {'key': params}
            if params['key'] in self.indexes[collection]:
                return  # Index already exists
            if 'id' not in params:
                params['id'] = uuid.uuid4().hex
            if ephemeral:
                index_store = None
            else:
                index_store = self.get_index_store(collection, params['id'])

            index = self.IndexClass(params, serializer=lambda x: self.serialize(x, autosave=False),
                                    deserializer=lambda x: self.deserialize(x),
                                    store=index_store, unique=unique)
            self.indexes[collection][params['key']] = index

            if collection not in self._config['indexes']:
                self._config['indexes'][collection] = {}

            if not ephemeral:
                self._config['indexes'][collection][params['key']] = params
                self.save_config()

            indexes.append(index)
            # if the index failed to load from disk we rebuild it
            if not index.loaded:
                keys.append(params['key'])

        self.rebuild_indexes(collection, keys)
        return indexes

    def get_collection_indexes(self, collection):
        return self.indexes[collection] if collection in self.indexes else {}

    def encode_attributes(self, attributes):
        return self.SerializerClass.serialize(attributes)

    def decode_attributes(self, data):
        return self.SerializerClass.deserialize(data)

    def get_object(self, cls, key):
        collection = self.get_collection_for_cls(cls)
        store = self.get_collection_store(collection)
        try:
            data = self.deserialize(
                self.decode_attributes(store.get_blob(key)))
        except IOError:
            raise cls.DoesNotExist
        obj = self.create_instance(cls, data)
        return obj

    def update(self, obj, set_fields = None, unset_fields = None, update_obj = True):
        """
        We return the result of the save method (updates are not yet implemented here).
        """
        if set_fields:
            if isinstance(set_fields,(list,tuple)):
                set_attributes = {}
                for key in set_fields:
                    try:
                        set_attributes[key] = get_value(obj,key)
                    except KeyError:
                        pass
            else:
                set_attributes = set_fields
        else:
            set_attributes = {}
        if unset_fields:
            unset_attributes = unset_fields
        else:
            unset_attributes = []

        self.call_hook('before_update',obj,set_attributes,unset_attributes)

        if update_obj:
            for key,value in set_attributes.items():
                set_value(obj,key,value)
            for key in unset_attributes:
                delete_value(obj,key)

        return self.save(obj,call_hook = False)

    def save(self, obj,call_hook = True):

        if call_hook:
            self.call_hook('before_save',obj)

        collection = self.get_collection_for_obj(obj)
        indexes = self.get_collection_indexes(collection)
        store = self.get_collection_store(collection)

        if obj.pk is None:
            obj.autogenerate_pk()

        serialized_attributes = self.serialize(obj.attributes)
        data = self.encode_attributes(serialized_attributes)

        try:
            store_key = (
                self
                .get_pk_index(collection)
                .get_keys_for(obj.pk, include_uncommitted=True).pop()
            )
        except IndexError:
            store_key = uuid.uuid4().hex

        store.store_blob(data, store_key)

        for key, index in indexes.items():
            index.add_key(serialized_attributes, store_key)

        if self.config['autocommit']:
            self.commit()

        return obj

    def delete_by_store_keys(self, collection, store_keys):

        store = self.get_collection_store(collection)
        indexes = self.get_collection_indexes(collection)

        for store_key in store_keys:
            try:
                store.delete_blob(store_key)
            except (KeyError, IOError):
                pass
            for index in indexes.values():
                index.remove_key(store_key)

        if self.config['autocommit']:
            self.commit()

    def delete(self, obj):

        self.call_hook('before_delete',obj)

        collection = self.get_collection_for_obj(obj)
        primary_index = self.get_pk_index(collection)
        return self.delete_by_store_keys(
            collection, primary_index.get_keys_for(obj.pk))

    def get(self, cls, query):
        objects = self.filter(cls, query)
        if len(objects) == 0:
            raise cls.DoesNotExist
        elif len(objects) > 1:
            raise cls.MultipleDocumentsReturned
        return objects[0]

    def sort(self, cls_or_collection, keys, key, order=QuerySet.ASCENDING):

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        if not isinstance(key, list) and not isinstance(key, tuple):
            sort_keys = [(key, order)]
        else:
            sort_keys = key

        indexes = self.get_collection_indexes(collection)

        indexes_to_create = []
        for sort_key, order in sort_keys:
            if sort_key not in indexes:
                indexes_to_create.append(sort_key)

        self.create_indexes(cls, indexes_to_create, ephemeral=True)

        def sort_by_keys(keys, sort_keys):
            if not sort_keys:
                return keys
            (sort_key, order) = sort_keys[0]
            _sorted_keys = indexes[sort_key].sort_keys(keys, order)
            return [sort_by_keys(k, sort_keys[1:]) for k in _sorted_keys]

        def flatten(l):
            fl = []
            for elem in l:
                if isinstance(elem, list):
                    fl.extend(flatten(elem))
                else:
                    fl.append(elem)

            return fl

        return flatten(sort_by_keys(keys, sort_keys))

    def _canonicalize_query(self, query):

        """
        Transform the query dictionary to replace e.g. documents with __ref__ fields.
        """

        def transform_query(q):

            if isinstance(q, dict):
                nq = {}
                for key,value in q.items():
                    nq[key] = transform_query(value)
                return nq
            elif isinstance(q, (list,QuerySet,tuple)):
                return [transform_query(x) for x in q]
            elif isinstance(q,Document):
                collection = self.get_collection_for_obj(q)
                ref = "%s:%s" % (collection,q.pk)
                return ref
            else:
                return q

        return transform_query(query)

    def filter(self, cls_or_collection, query, initial_keys=None):

        if not isinstance(query, dict):
            raise AttributeError('Query parameters must be dict!')

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        store = self.get_collection_store(collection)
        indexes = self.get_collection_indexes(collection)
        compiled_query = compile_query(self._canonicalize_query(query))

        indexes_to_create = []

        def query_function(key, expression):
            if key is None:
                return QuerySet(
                    self,
                    cls,
                    store,
                    self.get_pk_index(collection).get_all_keys()
                )
            qs = QuerySet(
                self,
                cls,
                store,
                indexes[key].get_keys_for(expression)
            )
            return qs

        def index_collector(key, expressions):
            if (key not in indexes
                    and key not in indexes_to_create
                    and key is not None):
                indexes_to_create.append(key)
            return QuerySet(self, cls, store, [])

        # We collect all the indexes that we need to create
        compiled_query(index_collector)

        if indexes_to_create:
            self.create_indexes(cls, indexes_to_create, ephemeral=True)

        query_set = compiled_query(query_function)

        return query_set
