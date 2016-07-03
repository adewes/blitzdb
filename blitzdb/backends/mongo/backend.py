import abc
import six

from collections import defaultdict

from blitzdb.document import Document
from blitzdb.backends.base import Backend as BaseBackend
from blitzdb.backends.base import NotInTransaction
from blitzdb.backends.mongo.queryset import QuerySet
import uuid
import pymongo

import logging
import traceback

from blitzdb.helpers import get_value,set_value,delete_value

logger = logging.getLogger(__name__)

class DotEncoder(object):

    DOT_MAGIC_VALUE = ":a5b8afc131:"

    @classmethod
    def encode(cls,obj,path):
        def replace_key(key):
            if isinstance(key,six.string_types):
                return key.replace(".", cls.DOT_MAGIC_VALUE)
            return key
        if isinstance(obj,dict):
            return dict([(replace_key(key),value) for key, value in obj.items()])
        return obj

    @classmethod
    def decode(cls,obj):
        if isinstance(obj,dict):
            return {key.replace(cls.DOT_MAGIC_VALUE, "."): value for key, value in obj.items()}
        return obj

class Backend(BaseBackend):

    """
    A MongoDB backend.

    :param db: An instance of a `pymongo.database.Database <http://api.mongodb.org/python/current/api/pymongo/database.html>`_ class

    Example usage:

    .. code-block:: python

        from pymongo import connection
        from blitzdb.backends.mongo import Backend as MongoBackend

        c = connection()
        my_db = c.test_db

        #create a new BlitzDB backend using a MongoDB database
        backend = MongoBackend(my_db)
    """

    standard_encoders = BaseBackend.standard_encoders + [DotEncoder]

    def __init__(self, db, autocommit=False, use_pk_based_refs = True,**kwargs):
        super(Backend, self).__init__(**kwargs)
        self.db = db
        self._autocommit = autocommit
        self._save_cache = defaultdict(lambda: {})
        self._delete_cache = defaultdict(lambda: {})
        self._update_cache = defaultdict(lambda: {})
        self._use_pk_based_refs = use_pk_based_refs
        self.in_transaction = False

    def begin(self):
        if self.in_transaction:  # we're already in a transaction...
            self.commit()
        self.in_transaction = True

    def rollback(self,transaction = None):
        if not self.in_transaction:
            raise NotInTransaction("Not in a transaction!")
        
        self._save_cache = defaultdict(lambda: {})
        self._delete_cache = defaultdict(lambda: {})
        self._update_cache = defaultdict(lambda: {})
        
        self.in_transaction = False

    def commit(self,transaction = None):
        try:
            for collection, cache in self._save_cache.items():
                for pk, attributes in cache.items():
                    try:
                        self.db[collection].save(attributes)
                    except:
                        logger.error("Error when saving the document with pk {0} in collection {1}".format(attributes['pk'], collection))
                        logger.error("Attributes (excerpt):" + str(dict(attributes.items()[:100])))
                        raise

            for collection, cache in self._delete_cache.items():
                for pk in cache:
                    self.db[collection].remove({'_id': pk})

            for collection, cache in self._update_cache.items():
                for pk, attributes in cache.items():
                    update_dict = {}
                    for key in ('$set', '$unset'):
                        if key in attributes and attributes[key]:
                            update_dict[key] = attributes[key]
                    if update_dict:
                        self.db[collection].update({'_id': pk}, update_dict)
        finally:
            #regardless what happens in the 'commit' operation, we clear the cache
            self._save_cache = defaultdict(lambda: {})
            self._delete_cache = defaultdict(lambda: {})
            self._update_cache = defaultdict(lambda: {})

            self.in_transaction = True

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        if value not in (True, False):
            raise TypeError("Value must be boolean!")
        self._autocommit = value

    def delete_by_primary_keys(self, cls, pks):
        collection = self.get_collection_for_cls(cls)
        if self.autocommit:
            for pk in pks:
                self.db[collection].remove({'_id': pk})
        else:
            self._delete_cache[collection].update(dict([(pk, True) for pk in pks]))

    def delete(self, obj):

        self.call_hook('before_delete',obj)

        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            raise obj.DoesNotExist
        if self.autocommit:
            self.db[collection].remove({'_id': obj.pk})
        else:
            self._delete_cache[collection][obj.pk] = True
            if obj.pk in self._save_cache[collection]:
                del self._save_cache[collection][obj.pk]

    def save_multiple(self, objs):
        if not objs:
            return
        serialized_attributes_list = []
        collection = self.get_collection_for_cls(objs[0].__class__)
        for obj in objs:
            self.call_hook('before_save',obj)
            if obj.pk == None:
                obj.pk = uuid.uuid4().hex
            serialized_attributes = self.serialize(obj.attributes)
            serialized_attributes['_id'] = obj.pk
            serialized_attributes_list.append(serialized_attributes)
        for attributes in serialized_attributes_list:
            if self.autocommit:
                self.db[collection].save(attributes)
            else:
                self._save_cache[collection][attributes['pk']] = attributes
                if attributes['pk'] in self._delete_cache[collection]:
                    del self._delete_cache[collection][attributes['pk']]

    def save(self, obj):
        return self.save_multiple([obj])

    def update(self, obj, set_fields=None, unset_fields=None, update_obj=True):

        collection = self.get_collection_for_cls(obj.__class__)

        if obj.pk == None:
            raise obj.DoesNotExist("update() called on document without primary key!")

        def serialize_fields(fields):


            if isinstance(fields, (list,tuple)):
                update_dict = {}
                for key in fields:
                    try:
                        update_dict[key] = get_value(obj,key)
                    except KeyError:
                        pass
            elif isinstance(fields,dict):
                update_dict = fields.copy()
            else:
                raise TypeError("fields must be a list/tuple!")

            return update_dict

        if set_fields:
            set_attributes = serialize_fields(set_fields)
        else:
            set_attributes = {}

        if unset_fields:
            unset_attributes = list(unset_fields)
        else:
            unset_attributes = []

        self.call_hook('before_update',obj,set_attributes,unset_attributes)


        set_attributes = {key : self.serialize(value)
                                for key,value in set_attributes.items()}

        if update_obj:
            for key,value in set_attributes.items():
                set_value(obj,key,value)
            for key in unset_attributes:
                delete_value(obj,key)

        update_dict = {}

        if set_attributes:
            update_dict['$set'] = set_attributes
        if unset_attributes:
            update_dict['$unset'] = {key : '' for key in unset_attributes}

        if not update_dict:
            return #nothing to do...

        if self.autocommit:
            self.db[collection].update({'_id': obj.pk}, update_dict)
        else:
            if obj.pk in self._delete_cache[collection]:
                raise obj.DoesNotExist("update() on document that is marked for deletion!")
            if obj.pk in self._update_cache[collection]:
                update_cache = self._update_cache[collection][obj.pk]
                if set_attributes:
                    if '$set' not in update_cache:
                        update_cache['$set'] = {}
                    for key, value in set_attributes.items():
                        if '$unset' in update_cache and key in update_cache['$unset']:
                            del update_cache['$unset'][key]
                        update_cache['$set'][key] = value
                if unset_attributes:
                    if '$unset' not in update_cache:
                        update_cache['$unset'] = {}
                    for key in unset_attributes:
                        if '$set' in update_cache and key in update_cache['$set']:
                            del update_cache['$set'][key]
                        update_cache['$unset'][key] = ''
            else:
                self._update_cache[collection][obj.pk] = update_dict

    def serialize(self, obj, convert_keys_to_str=True, embed_level=0,
                  encoders=None, autosave=True, for_query=False,path = None):

        return super(Backend, self).serialize(obj, 
                                              convert_keys_to_str=convert_keys_to_str, 
                                              embed_level=embed_level, 
                                              encoders=encoders, 
                                              autosave=autosave,
                                              path=path,
                                              for_query=for_query)

    def create_indexes(self, cls_or_collection, params_list):
        for params in params_list:
            self.create_index(cls_or_collection, **params)

    def ensure_indexes(self, include_pk=True):
        for cls in self.classes:
            meta_attributes = self.get_meta_attributes(cls)
            if include_pk:
                self.create_index(cls, fields={'pk': 1},opts = {'unique' : True})
            if 'indexes' in meta_attributes:
                self.create_indexes(cls, meta_attributes['indexes'])

    def create_index(self, cls_or_collection, *args, **kwargs):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection

        if 'fields' not in kwargs:
            raise AttributeError("You must specify the 'fields' parameter when creating an index!")
        if 'opts' in kwargs:
            opts = kwargs['opts']
        else:
            opts = {}
        try:
            self.db[collection].ensure_index(list(kwargs['fields'].items()), **opts)
        except pymongo.errors.OperationFailure as failure:
            traceback.print_exc()
            #The index already exists with different options, so we drop it and recreate it...
            self.db[collection].drop_index(list(kwargs['fields'].items()))
            self.db[collection].ensure_index(list(kwargs['fields'].items()), **opts)

    def _canonicalize_query(self, query):

        """
        Transform the query dictionary to replace e.g. documents with __ref__ fields.
        """

        def transform_query(q):

            for encoder in self.query_encoders:
                q = encoder.encode(q,[])

            if isinstance(q, dict):
                nq = {}
                for key,value in q.items():
                    new_key = key
                    if isinstance(value,dict) and len(value) == 1 and list(value.keys())[0].startswith('$'):
                        if list(value.keys())[0] in ('$all','$in'):
                            if list(value.values())[0] and isinstance(list(value.values())[0][0],Document):
                                if self._use_pk_based_refs:
                                    new_key+='.pk'
                                else:
                                    new_key+='.__ref__'
                    elif isinstance(value,Document):
                        if self._use_pk_based_refs:
                            new_key+='.pk'
                        else:
                            new_key+='.__ref__'
                    nq[new_key] = transform_query(value)
                return nq
            elif isinstance(q, (list,QuerySet,tuple)):
                return [transform_query(x) for x in q]
            elif isinstance(q,Document):
                collection = self.get_collection_for_obj(q)
                if self._use_pk_based_refs:
                    return q.pk
                else:
                    return "%s:%s" % (collection,q.pk)
            else:
                return q

        return transform_query(query)

    def get(self, cls_or_collection, properties, raw=False, only=None):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        cls = self.get_cls_for_collection(collection)
        queryset = self.filter(cls_or_collection, properties, raw=raw, only=only)
        if len(queryset) == 0:
            raise cls.DoesNotExist
        elif len(queryset) > 1:
            raise cls.MultipleDocumentsReturned
        return queryset[0]

    def filter(self, cls_or_collection, query, raw=False, only=None):
        """
        Filter objects from the database that correspond to a given set of properties.

        See :py:meth:`blitzdb.backends.base.Backend.filter` for documentation of individual parameters

        .. note::

            This function supports most query operators that are available in MongoDB and returns 
            a query set that is based on a MongoDB cursor.

        """

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        canonical_query = self._canonicalize_query(query)

        args = {}

        if only:
            if isinstance(only,tuple):
                args['projection'] = list(only)
            else:
                args['projection'] = only

        return QuerySet(self, cls, self.db[collection].find(canonical_query, **args), raw=raw, only=only)
