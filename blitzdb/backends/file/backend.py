import blitzdb

from blitzdb.backends.file.queryset import QuerySet
from blitzdb.backends.file.store import TransactionalStore,Store
from blitzdb.backends.file.index import TransactionalIndex,Index
from blitzdb.backends.base import Backend as BaseBackend,NotInTransaction,InTransaction
from blitzdb.backends.file.serializers import PickleSerializer,JsonSerializer,MarshalSerializer
from blitzdb.backends.file.queries import compile_query

import six
import os
import os.path

import hashlib
import datetime
import uuid
import copy

from collections import defaultdict

store_classes = {
    'transactional' : TransactionalStore,
    'basic' : Store,
}

index_classes = {
    'transactional' : TransactionalIndex,
    'basic' : Index
}

serializer_classes = {
    'pickle' : PickleSerializer,
    'json' : JsonSerializer,
    'marshal' : MarshalSerializer
}

#will only be available if cjson is installed
try:
    from blitzdb.backends.file.serializers import CJsonSerializer
    serializer_classes['cjson'] = CJsonSerializer
except ImportError:
    pass

class DatabaseIndexError(BaseException):
    """
    Gets raised when the index of the database is corrupted (ideally this should never happen).

    To recover from this error, you can call the `rebuild_index` function of the file backend with the
    affected collection and key as parameters.
    """

class Backend(BaseBackend):

    """
    A file-based database backend. Uses flat files to store objects on the hard disk and file-based
    indexes to optimize querying.

    :param path: The path to the database. If non-existant it will be created
    :param config: The configuration dictionary. If not specified, Blitz will try to load it from disk.
                   If this fails, the default configuration will be used instead.

    .. warning::

                    It might seem tempting to use the `autocommit` config and not having to worry about calling
                    `commit` by hand. Please be advised that this can incur a significant overhead in write
                    time since a `commit` will trigger a complete rewrite of all indexes to disk.

    """

    #the default configuration values.
    default_config = {
        'indexes' : {},
        'store_class' : 'transactional',
        'index_class' : 'transactional',
        'index_store_class' : 'basic',
        'serializer_class' : 'json', 
        'autocommit' : False,
    }

    config_defaults = {}

    def __init__(self,path,config = None,overwrite_config = False,**kwargs):

        self._path = os.path.abspath(path)
        if not os.path.exists(path):
            os.makedirs(path)

        self.collections = {}
        self.stores = {}
        self.in_transaction = False
        self.indexes = defaultdict(lambda : {})
        self.index_stores = defaultdict(lambda : {})
        self.load_config(config,overwrite_config)

        super(Backend,self).__init__(**kwargs)

    @property
    def autocommit(self):
        return True if 'autocommit' in self.config and self.config['autocommit'] else False

    @autocommit.setter
    def autocommit(self,value):
        if not value in (True,False):
            raise TypeError("Value must be boolean!")
        self.config['autocommit'] = value

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

    def commit(self):
        """
        Commits all pending transactions to the database.

        .. admonition:: Warning

            This operation can be **expensive** in runtime if a large number of documents (>100.000) is contained
            in the database, since it will cause all database indexes to be written to disk.
        """
        for collection in self.collections:
            store = self.get_collection_store(collection)
            store.commit()
            indexes = self.get_collection_indexes(collection)
            for index in indexes.values():
                index.commit()
        self.in_transaction = False
        self.begin()

    def rebuild_index(self,collection,key):
        """
        Rebuild a given index using the objects stored in the database.

        :param collection: The name of the collection for which to rebuild the index
        :param key: The key of the index to be rebuilt
        """
        return self.rebuild_indexes(collection,[key])

    def create_index(self,cls_or_collection,params,ephemeral = False):
        """
        Creates a new index on the given collection or class with the given parameters.

        :param cls_or_collection: The name of the collection or the class for which to create an index
        :param params: The parameters of the index
        :param ephemeral: Whether to create a persistent or an ephemeral index

        `params` expects either a dictionary of parameters or a string value. In the latter case, it
        will interpret the string as the name of the key for which an index is to be created.

        If `ephemeral = True`, the index will be created only in memory and will not be written to
        disk when :py:meth:`.commit` is called. This is useful for optimizing query performance.

        ..notice::

           By default, BlitzDB will create ephemeral indexes for all keys over which you perform queries,
           so after you've run a query on a given key for the first time, the second run will usually be
           much faster.

        **Specifying keys**

        Keys can be specified just like in MongoDB, using a dot ('.') to specify nested keys.

        .. code-block:: python

           actor = Actor({'name' : 'Charlie Chaplin',
            'foo' : {'value' : 'bar'}})

        If you want to create an index on `actor['foo']['value']` , you can just say

        .. code-block:: python

           backend.create_index(Actor,'foo.value')

        .. warning::

            Transcendental indexes (i.e. indexes transcending the boundaries of referenced objects)
            are currently not supported by Blitz, which means you can't create an index on an attribute
            value of a document that is embedded in another document.

        """
        return self.create_indexes(cls_or_collection,[params],ephemeral = ephemeral)

    def get_pk_index(self,collection):
        """
        Returns the primary key index for a given collection:

        :param collection: the collection for which to return the primary index

        :returns: the primary key index of the given collection
        """

        cls = self.collections[collection]

        if not cls.get_pk_name() in self.indexes[collection]:
            self.create_index(cls.get_pk_name(),collection)
        return self.indexes[collection][cls.get_pk_name()]

    def load_config(self,config = None,overwrite_config = False):
        config_file = self._path+"/config.json"
        if os.path.exists(config_file):
            with open(config_file,"rb") as config_file:
                #configuration is always stored in JSON format
                self._config = JsonSerializer.deserialize(config_file.read())
        else:
            if config:
                self._config = config.copy()
            else:
                self._config = {}
        if overwrite_config and config:
            self._config.update(config)

        for key,value in self.default_config.items():
            if not key in self._config:
                self._config[key] = value
        if not 'version' in self._config:
            self._config['version'] = blitzdb.__version__
        self.save_config()

    def save_config(self):
        config_file = self._path+"/config.json"
        with open(config_file,"wb") as config_file:
            config_file.write(JsonSerializer.serialize(self._config))

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self,config):
        self._config = config
        self.save_config()
        
    @property
    def path(self):
        return self._path

    def get_collection_store(self,collection):
        if not collection in self.stores:
            self.stores[collection] = self.StoreClass({'path':self.path+"/"+collection+"/objects"})
        return self.stores[collection]

    def get_index_store(self,collection,store_key):
        if not store_key in self.index_stores[collection]:
            self.index_stores[collection][store_key] = self.IndexStoreClass({'path':self.path+"/"+collection+"/indexes/"+store_key})
        return self.index_stores[collection][store_key]

    def register(self,cls,parameters = None):
        super(Backend,self).register(cls,parameters)
        self.init_indexes(self.get_collection_for_cls(cls))

    def get_storage_key_for(self,obj):
        collection = self.get_collection_for_obj(obj)
        pk_index = self.get_pk_index(collection)
        try:
            return pk_index.get_keys_for(obj.pk)[0]
        except (KeyError,IndexError):
            raise obj.DoesNotExist

    def init_indexes(self,collection):
        cls = self.collections[collection]
        if collection in self._config['indexes']:
            #If not pk index is present, we create one on the fly...
            if not [idx for idx in self._config['indexes'][collection].values() if idx['key'] == cls.get_pk_name()]:
                self.create_index(collection,{'key':cls.get_pk_name()})
            
            #We sort the indexes such that pk is always created first...
            for index_params in sorted(self._config['indexes'][collection].values(),key = lambda x: 0 if x['key'] == cls.get_pk_name() else 1):
                index = self.create_index(collection,index_params)
        else:
            #If no indexes are given, we just create a primary key index...
            self.create_index(collection,{'key':cls.get_pk_name()})

    def rebuild_indexes(self,collection,keys):
        if not keys:
            return
        all_objects = self.filter(collection,{})
        for key in keys:
            index = self.indexes[collection][key]
            index.clear()
        for key in keys:
            index = self.indexes[collection][key]
            for obj in all_objects:
                index.add_key(obj.attributes,obj._store_key)
            index.commit()

    def create_indexes(self,cls_or_collection,params_list,ephemeral = False):
        indexes = []
        keys = []

        if not params_list:
            return

        if not isinstance(cls_or_collection, six.string_types):
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

            index = self.IndexClass(params,serializer = lambda x:self.serialize(x,autosave = False),deserializer = lambda x : self.deserialize(x),store = index_store)
            self.indexes[collection][params['key']] = index

            if not collection in self._config['indexes']:
                self._config['indexes'][collection] = {}

            if not ephemeral:
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
        return self.SerializerClass.serialize(attributes)

    def decode_attributes(self,data):
        return self.SerializerClass.deserialize(data)

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
            store_key = self.get_pk_index(collection).get_keys_for(obj.pk,include_uncommitted = True).pop()
        except IndexError:
            store_key = uuid.uuid4().hex
    
        store.store_blob(data,store_key)

        for key,index in indexes.items():
            index.add_key(obj.attributes,store_key)

        if self.config['autocommit']:
            self.commit()

        return obj

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
        
        if self.config['autocommit']:
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

    def sort(self,cls_or_collection,keys,key,order = QuerySet.ASCENDING):

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        if not isinstance(key,list) and not isinstance(key,tuple):
            sort_keys = [(key,order)]
        else:
            sort_keys = key

        indexes = self.get_collection_indexes(collection)
        
        indexes_to_create = []
        for sort_key,order in sort_keys:
            if not sort_key in indexes:
                indexes_to_create.append(sort_key)

        self.create_indexes(cls,indexes_to_create,ephemeral = True)

        def sort_by_keys(keys,sort_keys):
            if not sort_keys:
                return keys
            (sort_key,order) = sort_keys[0]
            _sorted_keys =  indexes[sort_key].sort_keys(keys,order)
            return [sort_by_keys(k,sort_keys[1:]) for k in _sorted_keys]

        def flatten(l):
            fl = []
            [fl.extend(flatten(elem)) if isinstance(elem,list) else fl.append(elem) for elem in l]
            return fl

        return flatten(sort_by_keys(keys,sort_keys))

    def filter(self,cls_or_collection,query,sort_by = None,limit = None,offset = None,initial_keys = None):

        if not isinstance(query,dict):
            raise AttributeError("Query parameters must be dict!")

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        store = self.get_collection_store(collection)
        indexes = self.get_collection_indexes(collection)
        compiled_query = compile_query(self.serialize(query,autosave = False))

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
    
        if indexes_to_create:
            self.create_indexes(cls,indexes_to_create,ephemeral = True)
    
        return compiled_query(query_function)

