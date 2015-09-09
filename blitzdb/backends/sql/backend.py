import abc
import six
import uuid
import re
import traceback
import logging

logger = logging.getLogger(__name__)

from types import LambdaType
from collections import defaultdict

from ...document import Document
from ..base import Backend as BaseBackend
from ..base import NotInTransaction,DoNotSerialize
from ..file.serializers import JsonSerializer
from .queryset import QuerySet
from .relations import ListProxy,ManyToManyProxy

from blitzdb.fields import (ForeignKeyField,
                            ManyToManyField,
                            CharField,
                            EnumField,
                            IntegerField,
                            TextField,
                            FloatField,
                            ListField,
                            BooleanField,
                            BinaryField,
                            DateField,
                            DateTimeField,
                            BaseField
                            )

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import MetaData,Table,Column,ForeignKey,UniqueConstraint
from sqlalchemy.types import (Integer,
                              VARCHAR,
                              String,
                              Float,
                              Enum,
                              Boolean,
                              Date,
                              DateTime,
                              Text,
                              LargeBinary,
                              Unicode)
from sqlalchemy.sql import select,insert,update,func,and_,or_,not_,expression,null,distinct
from sqlalchemy.ext.compiler import compiles
from .helpers import get_value, set_value, delete_value

@compiles(DateTime, "sqlite")
def compile_binary_sqlite(type_, compiler, **kw):
    return "VARCHAR(64)"

class ExcludedFieldsEncoder(object):

    def __init__(self,backend,collection):
        self.collection = collection
        self.backend = backend

    def encode(self,obj,path = []):
        if not path:
            return obj
        key = ".".join([str(p) for p in path])
        if key in self.backend._excluded_keys[self.collection]:
            raise DoNotSerialize
        return obj

class Backend(BaseBackend):

    """
    A SQL backend.

    :param db: An instance of a `sqlalchemy. 
    <http://www.sqlalchemy.org>`_ class

    Example usage:

    .. code-block:: python

        from sqlalchemy import create_engine
        from blitzdb.backends.sql import Backend as SQLBackend

        my_engine = create_engine(...)

        #create a new BlitzDB backend using a SQLAlchemy engine
        backend = SQLBackend(my_engine)
    """

    class Meta(BaseBackend.Meta):
        pass

    def __init__(self, engine, table_postfix = '',create_schema = False,**kwargs):
        super(Backend, self).__init__(**kwargs)

        self._engine = engine
        self._transactions = []

        self.table_postfix = table_postfix

        if create_schema:
            self.create_schema()

        self._conn = self._engine.connect()
        self._auto_transaction = False
        self.begin()

    @property
    def connection(self):
        return self._conn

    def get_field_type(self,field):
        m = {
            IntegerField : Integer,
            FloatField : Float,
            CharField : VARCHAR(60),
            EnumField : lambda field: Enum(*field.enums),
            TextField : Text,
            BooleanField: Boolean,
            BinaryField: LargeBinary,
            DateField: Date,
            DateTimeField: DateTime
        }
        for cls,t in m.items():
            if isinstance(field,cls):
                if isinstance(t,LambdaType):
                    return t(field)
                return t
        raise AttributeError("Invalid field type: %s" % field)

    def get_column_for_key(self,cls_or_collection,key):
        if isinstance(cls_or_collection,six.string_types):
            collection = cls_or_collection
        else:
            collection = self.get_collection_for_cls(cls_or_collection)
        try:
            return self._index_fields[collection][key]['column']
        except KeyError:
            raise KeyError("Invalid key %s for collection %s" % (key,collection))
    
    def get_table_columns(self,cls_or_collection):
        if isinstance(cls_or_collection,six.string_types):
            collection = cls_or_collection
        else:
            collection = self.get_collection_for_cls(cls_or_collection)
        return self._table_columns[collection]

    def get_key_for_column(self,cls_or_collection,key):
        if isinstance(cls_or_collection,six.string_types):
            collection = cls_or_collection
        else:
            collection = self.get_collection_for_cls(cls_or_collection)
        for column,params in self._index_fields[collection].items():
            if params['key'] == key:
                return column
        raise KeyError

    @property
    def metadata(self):
        return self._metadata

    def init_schema(self):

        self._collection_tables = {}
        self._index_tables = defaultdict(dict)
        self._relationship_tables = defaultdict(dict)
        self._index_fields = defaultdict(dict)
        self._list_indexes = defaultdict(dict)
        self._related_fields = defaultdict(dict)
        self._table_columns = defaultdict(dict)
        self._excluded_keys = defaultdict(dict)
        self._foreign_key_backrefs = defaultdict(dict)
        self._many_to_many_backrefs = defaultdict(dict)

        def add_backref(collection,related_collection,column_name,field,many_to_many = False):
            if field.backref:
                backref_name = field.backref
            else:
                backref_name = 'related_%s' % (collection)

            if many_to_many:
                backref_dict = self._many_to_many_backrefs
            else:
                backref_dict = self._foreign_key_backrefs

            if backref_name in backref_dict[related_collection]:
                raise AttributeError("Backref %s for collection %s clashes with existing backref for collection %s!" % (backref_name,related_collection,backref_dict[related_collection][backref_name]['collection']))

            backref_dict[related_collection][backref_name] = {'collection' : collection,
                                                              'field' : field,
                                                              'column' : column_name
                                                             }

        def add_foreign_key_field(collection,cls,key,field):
            field_name = key
            column_name = key.replace('.','_')
            self._excluded_keys[collection][key] = True
            if isinstance(field.related,six.string_types):
                related_collection = self.get_collection_for_cls_name(field.related)
            else:
                related_collection = self.get_collection_for_cls(field.related)
            related_class = self.get_cls_for_collection(related_collection)

            add_backref(collection,related_collection,column_name,field)

            column = Column(column_name,self.get_field_type(related_class.Meta.PkType),ForeignKey('%s%s.pk' % (related_collection,self.table_postfix)),index=True,nullable = True if field.nullable else False)
            params = {'field' : field,
                      'key' : key,
                      'column' : column_name,
                      'collection' : related_collection,
                      'class' : related_class,
                      'type' : self.get_field_type(related_class.Meta.PkType)
                      }
            self._related_fields[collection][field_name] = params
            self._table_columns[collection][field_name] = params
            extra_columns.append(column)

            if field.unique:
                name = 'unique_%s_%s' % (collection,column_name)
                extra_columns.append(UniqueConstraint(column_name,name = name))

        def add_many_to_many_field(collection,cls,key,field):

            if isinstance(field.related,(list,tuple)):
                raise AttributeError("Currently not supported!")

            field_name = key
            column_name = key.replace('.','_')
            self._excluded_keys[collection][key] = True
            if isinstance(field.related,six.string_types):
                related_collection = self.get_collection_for_cls_name(field.related)
            else:
                related_collection = self.get_collection_for_cls(field.related)
            related_class = self.get_cls_for_collection(related_collection)
            relationship_name = "%s_%s" % (collection,related_collection)

            add_backref(collection,related_collection,column_name,field,many_to_many = True)

            params = {'field' : field,
                      'key' : key,
                      'collection' : related_collection,
                      'class' : related_class,
                      'type' : self.get_field_type(related_class.Meta.PkType)
                     }
            extra_columns = [
                UniqueConstraint('pk_%s' % related_collection,'pk_%s' % collection)
                ]
            relationship_table = Table('%s%s' % (relationship_name,self.table_postfix),self._metadata,
                    Column('pk_%s' % related_collection,self.get_field_type(related_class.Meta.PkType),ForeignKey('%s%s.pk' % (related_collection,self.table_postfix)),index = True),
                    Column('pk_%s' % collection,self.get_field_type(cls.Meta.PkType),ForeignKey('%s%s.pk' % (collection,self.table_postfix)),index = True),
                    *extra_columns
                )
            params['relationship_table'] = relationship_table
            self._relationship_tables[collection][field_name] = relationship_table
            self._related_fields[collection][field_name] = params

        def add_list_field(collection,cls,key,field):
            self._excluded_keys[collection][key] = True
            column_name = key.replace('.','_')
            index_name = "%s_%s" % (collection,column_name)

            index_params = {'field' : field,
                            'key' : key,
                            'type' : self.get_field_type(field.type),
                            'column' : column_name}
            self._index_tables[collection][key] = Table('%s%s' % (index_name,self.table_postfix),self._metadata,
                    Column('pk',self.get_field_type(cls.Meta.PkType),ForeignKey('%s%s.pk' % (collection,self.table_postfix)),index = True),
                    Column(column_name,index_params['type'],index = True),
                    UniqueConstraint('pk',column_name,name = 'unique_%s_%s' % (collection,column_name))
                )
            self._list_indexes[collection][key] = index_params
#            self._index_fields[collection][key] = index_params
#            self._table_columns[collection][key] = index_params

        def add_field(collection,key,field):
            self._excluded_keys[collection][key] = True
            column_name = key.replace('.','_')
            index_params = {'field' : field,
                            'key' : key,
                            'type' : self.get_field_type(field),
                            'column' : column_name}
            self._index_fields[collection][key] = index_params
            self._table_columns[collection][key] = index_params
            extra_columns.append(Column(column_name,index_params['type'],index = field.indexed))

            if field.unique:
                name = 'unique_%s_%s' % (collection,column_name)
                extra_columns.append(UniqueConstraint(column_name,name = name))

        self._metadata = MetaData()

        for collection,cls in self.collections.items():
            index_params = {
                'field' : cls.Meta.PkType,
                'type' : self.get_field_type(cls.Meta.PkType),
                'column' : 'pk',
                'key' : 'pk'
            }
            self._index_fields[collection]['pk'] = index_params
            self._table_columns[collection]['pk'] = index_params
            self._excluded_keys[collection]['pk'] = True

            extra_columns = [Column('pk',self.get_field_type(cls.Meta.PkType),primary_key = True,index = True)]

            meta_attributes = self.get_meta_attributes(cls)

            for key,field in cls._fields.items():
                if field.key:
                    key = field.key
                if not isinstance(field,BaseField):
                    raise AttributeError("Not a valid field: %s = %s" % (key,field))
                if isinstance(field,ForeignKeyField):
                    add_foreign_key_field(collection,cls,key,field)
                elif isinstance(field,ManyToManyField):
                    add_many_to_many_field(collection,cls,key,field)
                elif isinstance(field,ListField):
                    add_list_field(collection,cls,key,field)
                else:
                    add_field(collection,key,field)

            if 'unique_together' in meta_attributes:
                for keys in meta_attributes['unique_together']:
                    name = 'unique_together_%s_%s' % (collection,'_'.join(keys))
                    extra_columns.append(UniqueConstraint(*keys,name = name))

            self._collection_tables[collection] = Table('%s%s' % (collection,self.table_postfix),self._metadata,
                    Column('data',LargeBinary),
                    *extra_columns
                )

    def get_collection_table(self,collection):
        return self._collection_tables[collection]

    def begin(self,use_auto = True):
        if not self._transactions:
            self._auto_transaction = True
        elif self._auto_transaction and use_auto:
            self._auto_transaction = False
            return self._transactions[0]
        self._transactions.append(self.connection.begin())
        return self._transactions[-1]

    def commit(self,transaction = None):
        if not self._transactions:#should never happen
            raise AttributeError("Not in a transaction!")
        last_transaction = self._transactions.pop()
        if transaction is not None and last_transaction is not transaction:
            raise AttributeError("Transactions do not match!")
        last_transaction.commit()
        #if we have committed the last transaction, we open a new one
        if not self._transactions:
            self.begin()

    def rollback(self,transaction = None):
        if not self._transactions:
            raise AttributeError("Not in a transaction!")
        last_transaction = self._transactions.pop()
        if not self._auto_transaction and transaction is not None and last_transaction is not transaction:
            raise AttributeError("Transactions do not match!")
        last_transaction.rollback()
        #we roll back ALL transactions.
        self._transactions = []
        self.begin()

    def transaction(self,use_auto = True):
        """
        This returns a context guard which will automatically open and close a transaction
        """

        class TransactionManager(object):

            def __init__(self,backend,use_auto = True):
                self.use_auto = use_auto
                self.backend = backend

            def __enter__(self):
                self.transaction = self.backend.begin(use_auto = self.use_auto)

            def __exit__(self,exc_type,exc_value,traceback_obj):
                if exc_type:
                    self.backend.rollback(self.transaction)
                else:
                    self.backend.commit(self.transaction)

        return TransactionManager(self,use_auto = use_auto)

    def close_connection(self):
        return self.connection.close()

    def create_schema(self,indexes = None):
        self.init_schema()
        self._metadata.create_all(self._engine,checkfirst = True)

    def drop_schema(self):
        self.init_schema()
        self._metadata.drop_all(self._engine,checkfirst = True)

    def delete(self, obj):

        self.call_hook('before_delete',obj)

        if obj.pk == None:
            raise obj.DoesNotExist
        
        self.filter(obj.__class__,{'pk' : obj.pk}).delete()

    def update(self,obj,set_fields=None, unset_fields=None, update_obj=True):

        if set_fields is None:
            set_fields = {}

        if unset_fields is None:
            unset_fields = ()

        if isinstance(set_fields,(list,tuple)):
            set_fields_dict = {}
            for key in set_fields:
                set_fields_dict[key] = get_value(obj,key)
            set_fields = set_fields_dict

        if not isinstance(set_fields,dict):
            raise TypeError("set_fields must be a dictionary")

        if not isinstance(unset_fields,(tuple,list)):
            raise TypeError("unset_fields must be a tuple or a list")

        self.call_hook('before_update',obj,set_fields,unset_fields)

        for key in unset_fields:
            set_value(obj,key,None)

        for key,value in set_fields.items():
            obj[key] = value

        self.save(obj,call_hook = False)
        return obj

    def serialize_json(self,data):
        return JsonSerializer.serialize(data)

    def deserialize_json(self,data):
        if data and data != '{}':
            return JsonSerializer.deserialize(data)
        return {}

    def save(self,obj,autosave_dependent = True,call_hook = True):

        if call_hook:
            self.call_hook('before_save',obj)

        collection = self.get_collection_for_cls(obj.__class__)
        table = self._collection_tables[collection]

        """
        Document save strategy:

        - Retrieve values for simple embedded index fields
        - Store object data with index fields in DB
        - Retrieve values for list index fields
        - Store each list value in the index table
        - Retrieve related objects
        - Store related objects in the DB
        """

        pk_type = self._index_fields[collection]['pk']['type']

        deletes = []
        inserts = []

        def serialize_and_update_indexes(obj,d):
            for index_field,index_params in self._index_fields[collection].items():
                try:
                    value = get_value(obj,index_field)
                    if isinstance(index_params['field'],ListField):
                        #to do: check if this is a RelatedList
                        table = self._index_tables[collection][index_field]
                        deletes.append(table.delete().where(table.c['pk'] == expression.cast(obj.pk,pk_type)))
                        for element in value:
                            ed = {
                                'pk' : expression.cast(obj.pk,pk_type),
                                index_params['column'] : expression.cast(element,index_params['type']),
                            }
                            inserts.append(table.insert().values(**ed))
                    else:
                        if value is None:
                            if not index_params['field'].nullable:
                                raise ValueError("No value for %s given, but this is a mandatory field!" % index_field['key'])
                            d[index_params['column']] = null()
                        else:
                            d[index_params['column']] = expression.cast(value,index_params['type'])
                except KeyError:
                    if not isinstance(index_params['field'],ListField):
                        if not index_params['field'].nullable:
                            raise ValueError("No value for %s given, but this is a mandatory field!" % index_field['key'])
                        d[index_params['column']] = null()

        def serialize_and_update_relations(obj,d):
            for related_field,relation_params in self._related_fields[collection].items():
                try:
                    value = get_value(obj,related_field)
                    if isinstance(relation_params['field'],ManyToManyField):
                        relationship_table = self._relationship_tables[collection][related_field]
                        deletes.append(relationship_table.delete().where(relationship_table.c['pk_%s' % collection] == expression.cast(obj.pk,pk_type)))
                        for element in value:
                            if not isinstance(element,Document):
                                raise AttributeError("ManyToMany field %s contains an invalid value!" % related_field)
                            if element.pk is None:
                                if autosave_dependent:
                                    self.save(element)
                                else:
                                    raise AttributeError("Related document in field %s has no primary key!" % related_field)
                            ed = {
                                'pk_%s' % collection : obj.pk,
                                'pk_%s' % relation_params['collection'] : element.pk,
                            }
                            inserts.append(relationship_table.insert().values(**ed))
                    elif isinstance(relation_params['field'],ForeignKeyField):
                        if value is None:
                            if not relation_params['field'].nullable:
                                raise AttributeError("Field %s cannot be None!" % related_field)
                            d[relation_params['column']] = None
                        elif not isinstance(value,Document):
                            raise AttributeError("Field %s must be a document!" % related_field)
                        else:
                            if value.pk is None:
                                if autosave_dependent:
                                    self.save(value)
                                else:
                                    raise AttributeError("Related document in field %s has no primary key!" % related_field)
                            d[relation_params['column']] = expression.cast(value.pk,relation_params['type'])

                except KeyError:
                    #this index value does not exist in the object
                    pass

        with self.transaction(use_auto = False):

            insert = False
            if not obj.pk:
                obj.pk = uuid.uuid4().hex
                insert = True

            d = {'data' : self.serialize_json(self.serialize(obj.attributes,
                    encoders = [ExcludedFieldsEncoder(self,collection)])),
                 'pk' : expression.cast(obj.pk,pk_type)}

            serialize_and_update_indexes(obj,d)
            serialize_and_update_relations(obj,d)

            #if we got an object with a PK, we try to perform an UPDATE operation
            if not insert:
                update = self._collection_tables[collection].update().values(**d).where(table.c.pk == obj.pk)
                result = self.connection.execute(update)

            #if we did not get a PK the UPDATE did not match any rows, we perform an INSERT instead
            if insert or not result.rowcount:
                insert = self._collection_tables[collection].insert().values(**d)
                result = self.connection.execute(insert)

            for delete in deletes:
                self.connection.execute(delete)

            for insert in inserts:
                self.connection.execute(insert)

            return obj

    def get_include_joins(self,cls,includes,excludes = None):
        collection = self.get_collection_for_cls(cls)

        include_params = {'joins' : {},
                          'fields' : {},
                          'collection' : collection,
                          'table' : self._collection_tables[collection]
                          }

        include_list = [include_params]

        def resolve_include(include,collection,d):
            if isinstance(include,(tuple,list)):
                if len(include) >= 2:
                    main_include,sub_includes = include[0],include[1:]
                else:
                    main_include = include[0]
                    sub_includes = None
            else:
                main_include = include
                sub_includes = None
            for key,params in self._related_fields[collection].items():
                if main_include == key:
                    if not key in d['joins']:
                        d['joins'][key] = {'relation' : params,
                                           'table' : self._collection_tables[params['collection']],
                                           'collection' : params['collection'],
                                           'joins' : {},
                                           'fields' : {}}
                        include_list.append(d['joins'][key])
                    if sub_includes:
                        for sub_include in sub_includes:
                            resolve_include(sub_include,params['collection'],d['joins'][key])
                    break
            else:
                #if we ask for github_data and github_data.full_name is an index field, we
                #need to fetch both the `data` field and the github_data_full_name index field.
                for key,params in self._index_fields[collection].items():
                    if key == include:
                        d['fields'][key] = params['column']
                        break
                    elif key.startswith(include):
                        d['fields'][key] = params['column']
                else:
                    d['fields']['__data__'] = 'data'

        for include in includes:
            resolve_include(include,collection,include_params)

        excludes_set = set(excludes)
        for include in include_list:
            if not include['fields']:
                include['fields']['__data__'] = 'data'
                include['lazy'] = False
                for key,params in self._table_columns[include['collection']].items():
                    if key in excludes:
                        include['lazy'] = True
                        continue
                    include['fields'][key] = params['column']
            else:
                if not 'pk' in include['fields']:
                    include['fields']['pk'] = 'pk'
                if len(include['fields']) < len(self._index_fields[include['collection']])+1:
                    include['lazy'] = True
                else:
                    include['lazy'] = False

        return include_params

    def deserialize(self, obj, encoders=None,create_instance = True):
        return super(Backend, self).deserialize(obj,encoders = encoders,create_instance = create_instance)

    def serialize(self, obj, convert_keys_to_str=True, embed_level=0, encoders=None,**kwargs):
        """
        Only serialize fields that are not associate with a relation/backref!
        """

        return super(Backend, self).serialize(obj,
                                              convert_keys_to_str=convert_keys_to_str, 
                                              embed_level=embed_level,
                                              encoders = (encoders if encoders else []),
                                              **kwargs)

    def map_index_fields(self,collection_or_class,attributes):

        incomplete = False

        if isinstance(collection_or_class,six.string_types):
            collection = collection_or_class
        else:
            collection = self.get_collection_for_cls(collection_or_class)

        data = {}
        for field_name,params in self._index_fields[collection].items():
            if not params['column'] in attributes:
                incomplete = True
            else:
                set_value(data,params['key'],attributes[params['column']])

        return data,incomplete

    def deserialize_db_data(self,data):
        if not isinstance(data,dict):
            raise TypeError
        if not '__lazy__' in data:
            raise AttributeError("__lazy__ attribute not specified!")
        lazy = data['__lazy__']
        if '__data__' in data:
            d = self.deserialize_json(data['__data__'])
            #We delete excluded key values from the data, so that no poisoning can take place...
            for key in self._excluded_keys:
                delete_value(d,key)
        else:
            d = {}
        for key,value in data.items():
            if key in ('__data__','__lazy__'):
                continue
            set_value(d,key,value)
        return d,lazy

    def create_instance(self, collection_or_class,attributes,lazy = False):

        if isinstance(collection_or_class,six.string_types):
            collection = collection_or_class
        else:
            collection = self.get_collection_for_cls(collection_or_class)

        data = attributes
        #we create the object first
        obj = super(Backend,self).create_instance(collection_or_class,{},call_hook = False,lazy = lazy)

        #now we update the data dictionary with foreign key fields...
        for field_name,params in self._list_indexes[collection].items():
            set_value(data,params['key'],ListProxy(obj,field_name,params))

        for field_name,params in self._related_fields[collection].items():
            if isinstance(params['field'],ManyToManyField):
                try:
                    queryset = QuerySet(self,
                                        self._collection_tables[params['collection']],
                                        self.get_cls_for_collection(params['collection']),
                                        objects = get_value(data,params['key']))
                except KeyError:
                    queryset = None
                #check if we have data for this ManyToMany proxy. If yes, pass it along!
                set_value(data,params['key'],ManyToManyProxy(obj,field_name,params,queryset = queryset))
            elif isinstance(params['field'],ForeignKeyField):
                #check if we have data for this ForeignKey object. If yes, pass it along!
                try:
                    foreign_key_data = get_value(data,params['key'])
                except KeyError:
                    continue
                if foreign_key_data:
                    if not isinstance(foreign_key_data,dict):
                        foreign_key_data = {'pk' : foreign_key_data,'__lazy__' : True}
                    d,lazy_foreign_obj = self.deserialize_db_data(foreign_key_data)
                    foreign_obj = self.create_instance(params['class'],d,lazy = lazy_foreign_obj)
                else:
                    foreign_obj = None
                set_value(data,params['key'],foreign_obj)

        obj.attributes = data
        self.call_hook('after_load',obj)

        return obj

    def create_index(self, cls_or_collection, *args, **kwargs):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        self.db[collection].ensure_index(*args, **kwargs)

    def get(self, cls_or_collection, query,raw=False, only=None,include = None):

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        result = self.filter(cls_or_collection,query,raw = raw,only = only,include = include)
        try:
            return result[0]
        except IndexError:
            raise cls.DoesNotExist


    def filter(self, cls_or_collection, query, raw = False,only = None,include = None):
        """
        Filter objects from the database that correspond to a given set of properties.

        See :py:meth:`blitzdb.backends.base.Backend.filter` for documentation of individual parameters

        .. note::

            This function supports all query operators that are available in SQLAlchemy and returns a query set
            that is based on a SQLAlchemy cursor.

        Strategy:

        - Detect all index fields in the query.
        - For each index field, determine the type of the index.
            - If it is a ForeignKey index, use the pk element of the 

        SELECT query generation:

          - Non-indexed fields -> Raise an exception
          - Normal (in-table) index -> Make a query over the indexed field
          - List index -> Make a PK query over the index table
          - ForeignKey relation with related collection:
            - If `related.pk` or `related` used in query, directly query PK value in table
            - If deep field (e.g. `related.name`), make select over PK values of index field
              with result on query on the related table.

        """

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        table = self._collection_tables[collection]

        joins = defaultdict(dict)
        joins_list = []
        group_bys = []
        havings = []
        extra_fields = []

        def compile_query(collection,query,table = None,path = None):
            if path is None:
                path = []

            """
            This function emits a list of WHERE statements that can be used to retrieve 
            """

            if table is None:
                table = self._collection_tables[collection]

            where_statements  = []

            if any([True if key.startswith('$') else False for key in query.keys()]):
                #this is a special operator query
                if len(query) > 1:
                    raise AttributeError('Currently not supported!')
                operator = query.keys()[0][1:]
                if not operator in ('and','or','not'):
                    raise AttributeError("Non-supported logical operator: $%s" % operator)
                if operator in ('and','or'):
                    where_statements = [sq for expr in query['$%s' % operator] for sq in compile_query(collection,expr,path = path)]
                    if operator == 'and':
                        return [and_(*where_statements)]
                    else:
                        return [or_(*where_statements)]
                elif operator  == 'not':
                    return [not_(*compile_query(collection,query['$not'],table = table,path = path))]

            def compile_one_to_many_query(key,query,field_name,related_table,count_column,path):

                tail = key[len(field_name)+1:]
                if isinstance(query,Document) and not tail:
                    query = {'pk' : query.pk}
                elif not isinstance(query,dict):
                    if tail:
                        query = {tail : query}
                    else:
                        raise AttributeError
                        query = {'$all' : query}

                if len(query) == 1 and query.keys()[0].startswith('$'):
                    query_type = query.keys()[0][1:]
                    subquery = query.values()[0]

                    if query_type not in ('nin','in','all','elemMatch'):
                        raise AttributeError("Unsupported operator: %s" % query_type)
                    if query_type == 'elemMatch':
                        queries = compile_query(params['collection'],
                                                prepare_subquery(tail,query['$elemMatch']),
                                                table = related_table,
                                                path = path)
                        query_type = 'all'
                    else:
                        if len(subquery) and isinstance(subquery[0],dict) and len(subquery[0]) == 1 and \
                        subquery[0].keys()[0] == '$elemMatch':
                            queries = [sq for v in subquery for sq in compile_query(params['collection'],
                                                                                    prepare_subquery(tail,v['$elemMatch']),
                                                                                    table = related_table,
                                                                                    path = path)]
                        else:
                            queries = [sq for v in subquery for sq in compile_query(params['collection'],
                                                                                    prepare_subquery(tail,v),
                                                                                    table = related_table,
                                                                                    path = path)]
                else:
                    query_type = 'all'
                    queries = compile_query(params['collection'],query,
                                            table = related_table,
                                            path = path)

                #we add a count
                #extra_fields.append(func.count(related_table.c.pk))

                where_statement = or_(*queries)

                if query_type == 'nin':
                    where_statement = not_(where_statement)

                if query_type == 'all' and len(queries) > 1:
                    cnt = func.count(count_column)
                    havings.append(cnt == len(queries))

                return [where_statement]

            def compile_many_to_many_query(key,value,field_name,related_collection,relationship_table):

                """
                1) {'movies.title' : {'$all' : ['The Godfather','Apocalypse Now']}}
                2) {'movies.title' : {'$in' : ['The Godfather','Apocalypse Now']}}
                3) {'movies' : {'$in' : [the_godfather,apocalypse_now]}}
                4) {'movies' : the_godfather}
                5) {'movies' : {'$all' : [{'$elemMatch' : {'title' : 'The Godfather','language' : 'English'}},{'$elemMatch' : {'title' : 'Apocalypse Now'}}]}}

                SELECT 
                    [...],
                    COUNT(*) as movie_cnt -- 1)
                FROM 
                    actor
                LEFT JOIN --we join the relationship table
                    actor_movie ON actor.pk = actor_movie.actor_pk
                LEFT JOIN --we join the related table
                    movie ON actor_movie.actor_pk = movie.pk

                WHERE --we preform an IN query
                    ---
                    movie.title in ('The Godfather','Apocalypse Now') --1
                    ---
                    movie.pk in ([the_godfather.pk],[apocalypse_now.pk]) -- 3
                    ---
                    movie.pk = [the-godfather.pk] --4
                    ---
                    (movie.title = 'The Godfather' AND movie.language = 'English')
                    OR
                    movie.title = 'Apocalypse Now'
                GROUP BY 
                    actor.pk --we group by actor
                -- only in 1.)
                HAVING 
                    movie_cnt = 2 --only select elements where all conditions matched
               """
                related_table = self._collection_tables[related_collection]

                #this is a query for a document
                """
                Possible values:

                -A document, e.g. the_godfather (replace by pk matching)
                -A list of values, e.g. ('The Godfather','Apocalypse Now') (only valid if a tail is given)
                -A $elemMatch query, e.g. {'title' : 'Apocalypse Now'}
                -A list of $elemMatch queries

                Currently NOT valid:
                -A list of documents, e.g. [the_godfather,apocalypse_now] (NOT valid)

                """

                #to do: allow modifiers when using special queries (e.g. for regex)
                #Currently we only support $elemMatch, $all and $in operators

                new_path = path+[field_name]
                path_str = ".".join(new_path)

                if path_str in joins[relationship_table]:
                    relationship_table_alias = joins[relationship_table][path_str]
                else:
                    relationship_table_alias = relationship_table.alias()
                    joins[relationship_table][path_str] = relationship_table_alias
                    joins_list.append((relationship_table_alias,
                                       relationship_table_alias.c['pk_%s' % collection] == table.c['pk']))

                if path_str in joins[related_table]:
                    related_table_alias = joins[related_table][path_str]
                else:
                    related_table_alias = related_table.alias()
                    joins[related_table][path_str] = related_table_alias
                    joins_list.append((related_table_alias,relationship_table_alias.c['pk_%s' % related_collection] == related_table_alias.c['pk']))

                return compile_one_to_many_query(key,value,field_name,related_table_alias,relationship_table_alias.c['pk_%s' % collection],new_path)

            def prepare_subquery(tail,query_dict):
                d = {}
                if not tail:
                    if isinstance(query_dict,dict):
                        return query_dict.copy()
                    if not isinstance(query_dict,Document):
                        raise AttributeError("Must be a document!")
                    if not query_dict.pk:
                        raise AttributeError("Performing a query without a primary key!")
                    return {'pk' : query_dict.pk}
                if isinstance(query_dict,dict):
                    return {'.'.join([tail,k]) : v for k,v in query_dict.items()}
                else:
                    return {tail : query_dict}

            def prepare_special_query(field_name,params,query):
                column_name = params['column']
                if '$not' in query:
                    return [not_(*prepare_special_query(column_name,params,query['$not']))]
                elif '$in' in query:
                    return [table.c[column_name].in_(query['$in'])]
                elif '$nin' in query:
                    return [~table.c[column_name].in_(query['$nin'])]
                elif '$eq' in query:
                    return [table.c[column_name] == query['$eq']]
                elif '$ne' in query:
                    return [table.c[column_name] != query['$ne']]
                elif '$gt' in query:
                    return [table.c[column_name] > query['$gt']]
                elif '$gte' in query:
                    return [table.c[column_name] >= query['$gte']]
                elif '$lt' in query:
                    return [table.c[column_name] < query['$lt']]
                elif '$lte' in query:
                    return [table.c[column_name] <= query['$lte']]
                elif '$exists' in query:
                    if query['$exists']:
                        return [table.c[column_name] != None]
                    else:
                        return [table.c[column_name] == None]
                elif '$like' in query:
                    return [table.c[column_name].like(expression.cast(query['$like'],String))]
                elif '$regex' in query:
                    if not self._engine.url.drivername in ('postgres','mysql','sqlite'):
                        raise AttributeError("Regex queries not supported with %s engine!" % self._engine.url.drivername)
                    return [table.c[column_name].op('REGEXP')(expression.cast(query['$regex'],String))]
                else:
                    raise AttributeError("Invalid query!")

            foreign_key_backrefs = self._foreign_key_backrefs[collection]
            many_to_many_backrefs = self._many_to_many_backrefs[collection]
            #this is a normal, field-base query
            for key,value in query.items():
                for field_name,params in self._index_fields[collection].items():
                    if key == field_name:
                        #this is a list-indexed field
                        if isinstance(params['field'],ListField):
                            index_table = self._index_tables[collection][field_name]
                            if isinstance(value,dict):
                                related_query = lambda op: index_table.c[params['column']].in_([expression.cast(v,params['type']) for v in value[op]])
                                if '$in' in value:
                                    #do type-cast here?
                                    where_statements.append(related_query('$in'))
                                elif '$nin' in value:
                                    where_statements.append(~related_query('$nin'))
                                elif '$all' in value:
                                    pk_label = 'pk_%s' % field_name
                                    pk_column = index_table.c['pk'].label(pk_label)
                                    cnt = func.count(pk_column).label('cnt')
                                    subselect = select([cnt,pk_column],use_labels = True).where(related_query('$all')).group_by(pk_column)
                                    where_statements.append(table.c.pk.in_(select([subselect.columns[pk_label]]).where(subselect.columns['cnt'] == len(value['$all']))))
                                elif '$size' in value:
                                    raise NotImplementedError("$size operator is not yet implemented!")
                                else:
                                    raise AttributeError("Invalid query!")
                            else:
                                where_statements.append(index_table.c[params['column']] == expression.cast(value,params['type']))
                        else:
                            #this is a normal column index
                            if isinstance(value,re._pattern_type):
                                value = {'$regex' : value.pattern}
                            if isinstance(value,dict):
                                #this is a special query
                                where_statements.extend(prepare_special_query(field_name,params,value))
                            else:
                                #this is a normal value query
                                where_statements.append(table.c[params['column']] == expression.cast(value,params['type']))
                        break
                else:
                    #this is a non-indexed field! We try to find a relation...
                    for field_name,params in foreign_key_backrefs.items():
                        if key.startswith(field_name):
                            related_table = self._collection_tables[params['collection']]
                            relationship_params = self._related_fields[params['collection']][params['column']]
                            #join the table and
                            new_path = path + [field_name]
                            path_str = '.'.join(new_path)
                            if path_str in joins[related_table]:
                                related_table_alias = joins[related_table][path_str]
                            else:
                                related_table_alias = related_table.alias()
                                joins[related_table][path_str] = related_table_alias
                                joins_list.append((related_table_alias,related_table_alias.c[params['column']] == table.c['pk']))
                            #this is a one-to-many query
                            where_statements.extend(compile_one_to_many_query(key,value,field_name,related_table_alias,table.c.pk,new_path))
                            break
                    else:
                        #we check the many-to-many backreferences...
                        for field_name,params in many_to_many_backrefs.items():
                            if key.startswith(field_name):
                                relationship_table = self._relationship_tables[params['collection']][params['column']]
                                where_statements.extend(compile_many_to_many_query(key,value,field_name,params['collection'],relationship_table))
                                break
                        else:
                            #we check the normal relationships
                            for field_name,params in self._related_fields[collection].items():
                                if key.startswith(field_name):
                                    #ManyToManyField
                                    if isinstance(params['field'],ManyToManyField):
                                        relationship_table = self._relationship_tables[collection][field_name]
                                        where_statements.extend(compile_many_to_many_query(key,value,field_name,params['collection'],relationship_table))
                                    else:#this is a normal ForeignKey relation
                                        if key == field_name:
                                            if not isinstance(value,Document):
                                                raise AttributeError("ForeignKey query with non-document!")
                                            where_statements.append(table.c[params['column']] == value.pk)
                                        else:
                                            #we query a sub-field of the relation
                                            head,tail = key[:len(field_name)],key[len(field_name)+1:]
                                            related_table = self._collection_tables[params['collection']]
                                            new_path = path + [field_name]
                                            path_str = ".".join(new_path)
                                            if path_str in joins[related_table]:
                                                related_table_alias = joins[related_table][path_str]
                                            else:
                                                related_table_alias = related_table.alias()
                                                joins[related_table][path_str] = related_table_alias
                                                joins_list.append((related_table_alias,table.c[params['column']] == related_table_alias.c['pk']))
                                            where_statements.extend(compile_query(params['collection'],{tail : value},table = related_table_alias))
                                    break
                            else:
                                raise AttributeError("Query over non-indexed field %s in collection %s!" % (key,collection))
            return where_statements

        compiled_query = compile_query(collection,query)

        if len(compiled_query) > 1:
            compiled_query = and_(*compiled_query)
        elif compiled_query:
            compiled_query = compiled_query[0]
        else:
            compiled_query = None

        if joins_list:
            group_bys = [table.c.pk]

        return QuerySet(backend = self, table = table,
                        joins = joins_list,
                        cls = cls,
                        extra_fields = extra_fields,
                        condition = compiled_query,
                        raw = raw,
                        group_bys = group_bys,
                        only = only,
                        include = include,
                        havings = havings
                        )
