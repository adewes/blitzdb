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
from .relations import ManyToManyProxy

from blitzdb.fields import (ForeignKeyField,
                            ManyToManyField,
                            OneToManyField,
                            CharField,
                            EnumField,
                            IntegerField,
                            TextField,
                            FloatField,
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
from sqlalchemy.sql import select,insert,update,func,and_,or_,not_,expression,null
from sqlalchemy.ext.compiler import compiles
from blitzdb.helpers import get_value, set_value, delete_value

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

        self._engine_getter = engine
        self._engine = None
        self._schema_initialized = False
        self._relationship_classes = []
        self._transactions = []
        self.table_postfix = table_postfix

        self.init_schema()

        if create_schema:
            self.create_schema()

        self._conn = None

    @property
    def engine(self):
        if self._engine is None:
            if callable(self._engine_getter):
                self._engine = self._engine_getter()
            else:
                self._engine = self._engine_getter
        return self._engine

    @property
    def connection(self):
        if self._conn is None:
            raise AttributeError("No connection available!")
        return self._conn

    def create_connection(self):
        self._conn = self.engine.connect()

    @connection.deleter
    def connection(self):
        if self._conn:
            self._conn.close()
        self._conn = None

    def get_field_type(self,field,name = None):
        m = {
            IntegerField : Integer,
            FloatField : Float,
            CharField : lambda field: String(length = field.length),
            EnumField : lambda field: Enum(*field.enums,name = name,native_enum = field.native_enum),
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
            return self._table_columns[collection][key]['column']
        except KeyError:
            raise KeyError("Invalid key %s for collection %s" % (key,collection))
    
    def get_relationship_table(self,cls_or_collection,field):
        if isinstance(cls_or_collection,six.string_types):
            collection = cls_or_collection
        else:
            collection = self.get_collection_for_cls(cls_or_collection)
        return self._relationship_tables[collection][field]

    def get_table(self,cls_or_collection):
        if isinstance(cls_or_collection,six.string_types):
            collection = cls_or_collection
        else:
            collection = self.get_collection_for_cls(cls_or_collection)
        return self._collection_tables[collection]

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
        self._related_fields = defaultdict(dict)
        self._table_columns = defaultdict(dict)
        self._excluded_keys = defaultdict(dict)
        self._foreign_key_backrefs = defaultdict(dict)
        self._many_to_many_backrefs = defaultdict(dict)
        self._metadata = MetaData()
        self._schema_initialized = True

        for cls in self._relationship_classes:
            self.unregister(cls)

        self._relationship_classes = []
        for collection,cls in list(self.collections.items()):
            self.init_class_schema(cls,collection)


    def init_class_schema(self,cls,collection,table = None):

        def add_one_to_many_field(collection,cls,key,field,backref = None):

            if key in self._related_fields[collection]:
                raise AttributeError("Related one-to-many field %s of class %s already defined in collection %s by class %s (this might be a problem with a backreference)" % (key,cls.__name__,collection,self._related_fields[collection][key]['class'].__name__))

            if isinstance(field.related,six.string_types):
                related_collection = self.get_collection_for_cls_name(field.related)
            else:
                related_collection = self.get_collection_for_cls(field.related)
            related_class = self.get_cls_for_collection(related_collection)
            field_name = key
            column_name = key.replace('.','_')

            params = {
                'field' : field,
                'key' : key,
                'collection' : related_collection,
                'class' : related_class,
                'type' : self.get_field_type(cls.Meta.PkType),
                'is_backref' : True if backref is not None else False,
                'backref' : backref,
            }

            if backref is None:
                backref_key = field.backref or 'related_%s_%s' % (collection,column_name)
                params['backref'] = add_foreign_key_field(related_collection,related_class,backref_key,
                    ForeignKeyField(cls,key = backref_key))

            self._excluded_keys[collection][field_name] = True
            self._related_fields[collection][field_name] = params


        def add_foreign_key_field(collection,cls,key,field,backref = None):

            if key in self._related_fields[collection]:
                raise AttributeError("Related foreign key field %s already defined in collection %s (this might be a problem with a backreference)" % (key,collection))

            field_name = key
            column_name = key.replace('.','_')
            self._excluded_keys[collection][key] = True
            if isinstance(field.related,six.string_types):
                related_collection = self.get_collection_for_cls_name(field.related)
            else:
                related_collection = self.get_collection_for_cls(field.related)
            related_class = self.get_cls_for_collection(related_collection)
            column = Column(column_name,self.get_field_type(related_class.Meta.PkType),
                            ForeignKey('%s%s.pk' % (related_collection,self.table_postfix),name = '%s_%s_%s' % (collection,related_collection,column_name), ondelete = field.ondelete,use_alter = False),
                            index=True,nullable = True if field.nullable else False)

            params = {'field' : field,
                      'key' : key,
                      'column' : column_name,
                      'collection' : related_collection,
                      'class' : related_class,
                      'type' : self.get_field_type(related_class.Meta.PkType),
                      'is_backref' : True if backref is not None else False,
                      'backref' : backref,
                      }

            self._related_fields[collection][field_name] = params
            self._table_columns[collection][field_name] = params
            extra_columns.append(column)

            if field.unique:
                name = 'unique_%s_%s' % (collection,column_name)
                extra_columns.append(UniqueConstraint(column_name,name = name))

            if backref is None:
                backref_key = field.backref or 'related_%s_%s' % (collection,column_name)
                params['backref'] = add_one_to_many_field(related_collection,related_class,backref_key,
                                      OneToManyField(related = cls,
                                                     unique = field.unique,
                                                     key = backref_key),
                                                     backref = params)

            return params

        def add_many_to_many_field(collection,cls,key,field,backref = None):

            if isinstance(field.related,(list,tuple)):
                raise AttributeError("Currently not supported!")

            if key in self._related_fields[collection]:
                raise AttributeError("Related many-to-many field %s already defined in collection %s (this might be a problem with a backreference)" % (key,collection))

            field_name = key
            column_name = key.replace('.','_')
            self._excluded_keys[collection][key] = True

            if isinstance(field.related,six.string_types):
                related_collection = self.get_collection_for_cls_name(field.related)
            else:
                related_collection = self.get_collection_for_cls(field.related)
            related_class = self.get_cls_for_collection(related_collection)

            pk_field_name = field.field or 'pk_%s' % collection
            related_pk_field_name = field.related_field or 'pk_%s' % related_collection
            if related_pk_field_name == pk_field_name:#this can happen if we connect a given table with itself
                related_pk_field_name = related_pk_field_name+'_right'

            relationship_name = "%s_%s_%s" % (collection,related_collection,column_name)

            params = {'field' : field,
                      'key' : key,
                      'collection' : related_collection,
                      'class' : related_class,
                      'type' : self.get_field_type(related_class.Meta.PkType),
                      'is_backref' : True if backref is not None else False,
                      'backref' : backref
                     }

            if backref:
                relationship_table = backref['relationship_table']
            else:
                relationship_name = "%s_%s" % (collection,related_collection)
                extra_columns = [
                    UniqueConstraint('pk_%s' % related_collection,'pk_%s' % collection,name = '%s_%s_unique' % (relationship_name,column_name))
                    ]
                relationship_table = Table('%s%s' % (relationship_name,self.table_postfix),self._metadata,
                        Column(related_pk_field_name,self.get_field_type(related_class.Meta.PkType),ForeignKey('%s%s.pk' % (related_collection,self.table_postfix),name = "%s_%s" % (relationship_name,related_pk_field_name), ondelete = 'CASCADE',use_alter = False),index = True),
                        Column(pk_field_name,self.get_field_type(cls.Meta.PkType),ForeignKey('%s%s.pk' % (collection,self.table_postfix),name = "%s_%s" % (relationship_name,pk_field_name),ondelete = 'CASCADE',use_alter = False),index = True),
                        *extra_columns
                    )
                params['relationship_table'] = relationship_table


            self._relationship_tables[collection][field_name] = relationship_table
            self._related_fields[collection][field_name] = params

            if backref is None:
                backref_key = field.backref or 'related_%s_%s' % (collection,column_name)
                params['backref'] = add_many_to_many_field(related_collection,
                                       related_class,
                                       key = backref_key,
                                       field = ManyToManyField(cls,key = backref_key),
                                       backref = params)

                #We add an explicit relationship class for the relationship

                class RelationshipClass(Document):

                    class Meta(Document.Meta):
                        autoregister = False

                    from_class = cls
                    to_class = related_class

                RelationshipClass.__name__ = str("%s%s" % (collection.capitalize(),"".join([ "".join([kk.capitalize() for kk in k.split("_")]) for k in key.split(".")])))
                
                backref_name_left = '%s_%s_%s' % (collection,related_collection,column_name)
                backref_name_right = '%s_%s_%s' % (related_collection,collection,column_name)
                if backref_name_left == backref_name_right:
                    backref_name_right += '_right'
                RelationshipClass.fields[pk_field_name] = ForeignKeyField(cls,
                                                            backref = backref_name_left,
                                                            ondelete = 'CASCADE')
                RelationshipClass.fields[related_pk_field_name] =ForeignKeyField(related_class,
                                                            backref = backref_name_right,
                                                            ondelete = 'CASCADE')

                field.relationship_class = RelationshipClass

                #we append the class to the list of relationship classes so we can unregister it later
                #this is important when calling init_schema more than once...
                self._relationship_classes.append(RelationshipClass)

                self.register(RelationshipClass,parameters = {'collection' : relationship_name},overwrite = True)
                self.init_class_schema(RelationshipClass,relationship_name,table = relationship_table)

            return params

        def add_field(collection,key,field):
            self._excluded_keys[collection][key] = True
            column_name = key.replace('.','_')
            index_params = {'field' : field,
                            'key' : key,
                            'type' : self.get_field_type(field,name = '%s_%s' % (collection,column_name)),
                            'column' : column_name}
            self._index_fields[collection][key] = index_params
            self._table_columns[collection][key] = index_params
            column_args = {'index' : field.indexed,
                           'primary_key' : field.primary_key,
                           'nullable' : field.nullable}

            if field.default is not None:
                column_args['default'] = expression.cast(field.default,index_params['type'])

            if field.server_default is not None:
                column_args['server_default'] = expression.cast(field.default,index_params['type'])

            extra_columns.append(Column(column_name,index_params['type'],
                                        **column_args))

            if field.unique:
                name = 'unique_%s_%s' % (collection,column_name)
                extra_columns.append(UniqueConstraint(column_name,name = name))

        #if not primary key field is defined, we add one
        if not 'pk' in cls.fields:
            cls.fields['pk'] = cls.Meta.PkType

        extra_columns = []

        meta_attributes = self.get_meta_attributes(cls)

        for key,field in cls.fields.items():
            if not isinstance(field,BaseField):
                raise AttributeError("Not a valid field: %s = %s" % (key,field))
            if isinstance(field,ForeignKeyField):
                add_foreign_key_field(collection,cls,key,field)
            elif isinstance(field,ManyToManyField):
                add_many_to_many_field(collection,cls,key,field)
            elif isinstance(field,OneToManyField):
                add_one_to_many_field(collection,cls,key,field)
            else:
                add_field(collection,key,field)

        if 'unique_together' in meta_attributes:
            for keys in meta_attributes['unique_together']:
                columns = [self.get_column_for_key(collection,key) for key in keys]
                name = 'unique_together_%s_%s' % (collection,'_'.join(columns))
                extra_columns.append(UniqueConstraint(*columns,name = name))

        if table is None:
            table = Table('%s%s' % (collection,self.table_postfix),self._metadata,
                            Column('data',LargeBinary),
                            *extra_columns
                        )
        self._collection_tables[collection] = table

    def get_collection_table(self,collection):
        return self._collection_tables[collection]

    @property
    def current_transaction(self):
        if self._transactions:
            return self._transactions[-1]
        return None

    def begin(self):
        #if we do not have an active connection, we create one
        if self._conn is None:
            self.create_connection()
        self._transactions.append(self.connection.begin())
        return self._transactions[-1]

    def commit(self,transaction = None):
        if not self._transactions:#should never happen
            return
        #if transaction is not None and self._transactions[-1] is not transaction:
        #    #this is the wrong transaction object...
        #    return
        last_transaction = self._transactions.pop()
        last_transaction.commit()
        if not self._transactions:
            #we return the connection to the pool
            del self.connection

    def rollback(self,transaction = None):
        if not self._transactions:
            return
        if transaction is not None and self._transactions[-1] is not transaction:
            return
        last_transaction = self._transactions.pop()
        try:
            last_transaction.rollback()
            #we roll back ALL transactions.
            self._transactions = []
            #we return the connection to the pool
        finally:
            #we always delete the connection, even if the rollback has failed
            del self.connection

    def replace_engine(self,engine):
        self._engine = engine
        self._conn = None
        self._transactions = []

    def replace_engine_getter(self,engine_getter):
        self._engine_getter = engine_getter
        self._engine = None
        self._conn = None
        self._transactions = []

    def create_schema(self,indexes = None):
        if not self._schema_initialized:
            self.init_schema()
        self._metadata.create_all(self.engine,checkfirst = True)

    def drop_schema(self):
        self.init_schema()
        self._metadata.drop_all(self.engine,checkfirst = True)

    def delete(self, obj):

        self.call_hook('before_delete',obj)

        if obj.pk == None:
            raise obj.DoesNotExist
        
        self.filter(obj.__class__,{'pk' : obj.pk}).delete()

    def update(self,obj,set_fields=None, unset_fields=None, update_obj=True):

        if obj.pk is None:
            raise obj.DoesNotExist("Trying to update a document without a primary key!")

        if set_fields is None:
            set_fields = {}

        if unset_fields is None:
            unset_fields = []
        else:
            unset_fields = list(unset_fields)

        def flatten_set_fields(d,fd = None,path = None):
            if path is None:
                path = []
            if fd is None:
                fd = {}
            for key,value in d.items():
                if isinstance(value,dict):
                    flatten_set_fields(value,fd,path = path+[key])
                else:
                    fd[".".join(path+[key])] = value
            return fd

        collection = self.get_collection_for_cls(obj.__class__)
        table = self._collection_tables[collection]

        if isinstance(set_fields,(list,tuple)):
            set_fields_dict = {}
            for key in set_fields:
                try:
                    set_fields_dict[key] = get_value(obj,key)
                except KeyError:
                    set_fields_dict[key] = None
            set_fields = set_fields_dict

        set_fields = flatten_set_fields(set_fields)

        #if we set/unset `github_access_data`, we also set/unset `github_access_data.login` etc...
        for key in self._table_columns[collection]:
            for unset_field in unset_fields:
                if key.startswith(unset_field+'.') and not key in unset_fields:
                    unset_fields.append(key)
            for set_field in set_fields.keys():
                if key.startswith(set_field+'.') and not key in set_fields:
                    try:
                        set_fields[key] = get_value(obj,key)
                    except KeyError:
                        set_fields[key] = None

        self.call_hook('before_update',obj,set_fields,unset_fields)

        if update_obj:
            for key,value in set_fields.items():
                set_value(obj,key,value)
            for key in unset_fields:
                delete_value(obj,key)

        if not isinstance(set_fields,dict):
            raise TypeError("set_fields must be a dictionary")

        if not isinstance(unset_fields,(tuple,list)):
            raise TypeError("unset_fields must be a tuple or a list")


        index_fields = self._index_fields[collection]
        related_fields = self._related_fields[collection]
        pk_type = self._index_fields[collection]['pk']['type']

        deletes = []
        inserts = []

        with self.transaction(implicit = True):

            data_set_keys = {}
            data_unset_keys = set()
            update_dict = {}
            delete_keys = set()

            for key,value in set_fields.items():
                if not key in index_fields and not key in related_fields:
                    data_set_keys[key] = value
                    continue
                update_dict[key] = value

            for key in unset_fields:
                if not key in index_fields and not key in related_fields:
                    data_unset_keys.add(key)
                    continue
                #we set the value to None to "delete" it from the document.
                update_dict[key] = None

            update_dict['pk'] = obj.pk
            d = {}

            self._serialize_and_update_indexes(update_dict,collection,d,for_update = True)
            self._serialize_and_update_relations(update_dict,collection,d,deletes,
                                                 inserts,autosave_dependent = True,
                                                 for_update = True)

            if not 'pk' in set_fields or set_fields['pk'] is None:
                del d['pk']

            #if we have to update the JSON data
            if data_set_keys or data_unset_keys:
                result = self.connection.execute(select([table.c.data]).where(table.c.pk == expression.cast(obj.pk,pk_type)))
                data_row = result.fetchone()
                if data_row is None:
                    raise obj.DoesNotExist("Object does not exist!")
                data = self.deserialize_json(data_row[0])
                for key,value in data_set_keys.items():
                    set_value(data,key,value)
                for key in data_unset_keys:
                    delete_value(data,key)
                self.connection.execute(table.update()\
                                        .values({'data' : expression.cast(self.serialize_json(self.serialize(data)),LargeBinary)})\
                                        .where(table.c.pk == expression.cast(obj.pk,pk_type)))

            for delete in deletes:
                self.connection.execute(delete)

            for insert in inserts:
                self.connection.execute(insert)

            if d:
                update = table.update().values(**d).where(table.c.pk == expression.cast(obj.pk,pk_type))
                result = self.connection.execute(update)
                if not result.rowcount:
                    raise obj.DoesNotExist("Object does not exist!")

            return obj


    def serialize_json(self,data):
        return JsonSerializer.serialize(data)

    def deserialize_json(self,data):
        if data and data != '{}':
            return JsonSerializer.deserialize(data)
        return {}

    def _serialize_and_update_indexes(self,obj,collection,d,for_update = False):

        pk_type = self._index_fields[collection]['pk']['type']

        for index_field,index_params in self._index_fields[collection].items():
            try:
                if for_update:
                    value = obj[index_field]
                else:
                    value = get_value(obj,index_field)
                if value is None:
                    if not index_params['field'].nullable:
                        raise ValueError("Value for %s is `None`, but this is a mandatory field!" % index_field)
                    d[index_params['column']] = null()
                else:
                    d[index_params['column']] = expression.cast(value,index_params['type'])
            except KeyError:
                if for_update:
                    continue
                if index_params['field'].default is not None:
                    d[index_params['column']] = index_params['field'].default
                elif not index_params['field'].nullable:
                    raise ValueError("No value for %s given, but this is a mandatory field!" % index_field)
                else:
                    d[index_params['column']] = null()

    def _serialize_and_update_relations(self,obj,collection,d,deletes,inserts,autosave_dependent = True,for_update = False, save_cache=None):

        pk_type = self._index_fields[collection]['pk']['type']

        for related_field,relation_params in self._related_fields[collection].items():

            #we skip back-references...
            if relation_params.get('is_backref',None):
                continue

            try:
                if for_update:
                    value = obj[related_field]
                else:
                    value = get_value(obj,related_field)
                if isinstance(relation_params['field'],ManyToManyField):
                    if isinstance(value,ManyToManyProxy):
                        continue
                    relationship_table = self._relationship_tables[collection][related_field]
                    deletes.append(relationship_table.delete().where(relationship_table.c['pk_%s' % collection] == expression.cast(obj['pk'],pk_type)))
                    for element in value:
                        if not isinstance(element,Document):
                            raise AttributeError("ManyToMany field %s contains an invalid value!" % related_field)
                        if autosave_dependent and element.pk is None:
                            self.save(element, save_cache=save_cache)
                        if element.pk is None:
                            raise AttributeError("Related document in field %s has no primary key!" % related_field)
                        ed = {
                            'pk_%s' % collection : obj['pk'],
                            'pk_%s' % relation_params['collection'] : element.pk,
                        }
                        inserts.append(relationship_table.insert().values(**ed))
                elif isinstance(relation_params['field'],ForeignKeyField):
                    if value is None:
                        if not relation_params['field'].nullable:
                            raise AttributeError("Field %s cannot be None!" % related_field)
                        d[relation_params['column']] = null()
                    elif not isinstance(value,Document):
                        raise AttributeError("Field %s must be a document!" % related_field)
                    else:
                        if autosave_dependent and value.pk is None:
                            self.save(value, save_cache=save_cache)
                        if value.pk is None:
                            raise AttributeError("Related document in field %s has no primary key!" % related_field)
                        d[relation_params['column']] = expression.cast(value.pk,relation_params['type'])

            except KeyError:
                if for_update:
                    continue
                if isinstance(relation_params['field'],ForeignKeyField):
                    if not relation_params['field'].nullable:
                        raise ValueError("No value for %s given, but this is a mandatory field!" % relation_params['key'])
                    d[relation_params['column']] = null()


    def save(self,obj,autosave_dependent = True,call_hook = True, save_cache=None):

        if save_cache is None:
            save_cache = []

        if obj.lazy:
            raise AttributeError("Trying to save a lazy object!")

        if call_hook:
            self.call_hook('before_save',obj)

        collection = self.get_collection_for_cls(obj.__class__)
        table = self._collection_tables[collection]
        pk_type = self._index_fields[collection]['pk']['type']

        deletes = []
        inserts = []

        try:
            with self.transaction(implicit = True):

                save_cache.append((obj,obj.pk,obj.backend))

                is_insert = False
                if not obj.pk:
                    obj.pk = uuid.uuid4().hex
                    is_insert = True

                d = {'data' : expression.cast(self.serialize_json(self.serialize(obj.attributes,
                        encoders = [ExcludedFieldsEncoder(self,collection)])),LargeBinary),
                     'pk' : expression.cast(obj.pk,pk_type)}

                self._serialize_and_update_indexes(obj,collection,d)
                self._serialize_and_update_relations(obj,collection,d,deletes,inserts,autosave_dependent = autosave_dependent, save_cache=save_cache)

                #if we got an object with a PK, we try to perform an UPDATE operation

                if not is_insert:
                    update = self._collection_tables[collection].update().values(**d).where(table.c.pk == obj.pk)
                    result = self.connection.execute(update)

                #if we did not get a PK the UPDATE did not match any rows, we perform an INSERT instead
                if is_insert or not result.rowcount:
                    insert = self._collection_tables[collection].insert().values(**d)
                    result = self.connection.execute(insert)
                    is_insert = True

                for delete in deletes:
                    self.connection.execute(delete)

                for insert in inserts:
                    self.connection.execute(insert)

                #after saving an object, we initialize the relations
                obj.backend = self
                self.initialize_relations(obj)
                return obj
        except:
            #we restore all objects to the state they've been in before...
            for saved_obj,pk,backend in save_cache:
                saved_obj.pk = pk
                saved_obj.backend = backend
            raise

    def initialize_relations(self,obj,data = None):

        if data is None:
            data = obj.attributes

        collection = self.get_collection_for_cls(obj.__class__)

        for key,params in self._related_fields[collection].items():
            if isinstance(params['field'],ManyToManyField):
                try:
                    #to do: add proper select condition
                    objects = get_value(data,key)
                except KeyError:
                    objects = None
                if isinstance(objects,ManyToManyProxy):
                    continue #already initialized
                #check if we have data for this ManyToMany proxy. If yes, pass it along!
                set_value(data,key,ManyToManyProxy(obj,key,params,objects = objects))
            elif isinstance(params['field'],ForeignKeyField):
                #check if we have data for this ForeignKey object. If yes, pass it along!
                try:
                    foreign_key_data = get_value(data,key)
                except KeyError:
                    set_value(data,key,None)
                    continue
                if isinstance(foreign_key_data,Document):
                    continue #already initialized
                if foreign_key_data:
                    if not isinstance(foreign_key_data,dict):
                        foreign_key_data = {'pk' : foreign_key_data,
                                            '__lazy__' : True,
                                            '__collection__' : collection}
                    d,lazy_foreign_obj = self.deserialize_db_data(foreign_key_data)
                    foreign_obj = self.create_instance(params['class'],d,lazy = lazy_foreign_obj)
                else:
                    foreign_obj = None
                set_value(data,key,foreign_obj)
            elif isinstance(params['field'],OneToManyField):
                try:
                    objects = get_value(data,key)
                except KeyError:
                    objects = None
                if isinstance(objects,(QuerySet,Document)):
                    continue #already initialized
                table = self._collection_tables[params['collection']]
                related_table = self._collection_tables[params['backref']['collection']]
                qs = QuerySet(backend = self,
                        table = table,
                        cls = params['class'],
                        condition = table.c[params['backref']['column']] == expression.cast(data['pk'],params['type']),
                        objects = objects,
                        raw = False,
                        )
                if params['field'].unique:
                    if objects is not None:
                        try:
                            set_value(data,key,qs[0])
                        except IndexError:
                            set_value(data,key,None)
                    else:
                        def db_loader(params = params,qs = qs):
                            #warning: pass external parameters as default to 
                            #make sure that the function sees the correct closure
                            try:
                                obj = qs[0]
                            except IndexError:
                                raise params['class'].DoesNotExist
                            if len(qs) > 1:
                                raise params['class'].MultipleDocumentsReturned
                            return obj

                        set_value(data,key,params['class']({},lazy = True,db_loader = db_loader))
                else:
                    set_value(data,key,qs)

        obj.attributes = data

    def get_include_joins(self,cls,includes,excludes = None,order_by_keys = None):
        collection = self.get_collection_for_cls(cls)

        include_params = {'joins' : {},
                          'fields' : {},
                          'lazy' : False,
                          'collection' : collection,
                          'table' : self._collection_tables[collection]
                          }

        include_list = [include_params]

        def resolve_include(include,collection,d,path = None):
            if path is None:
                path = []
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
                #by adding the . we make sure that we won't inlcude e.g. committer_date
                #if committer_date_ts is asked for.
                for key,params in self._index_fields[collection].items():
                    if key == main_include:
                        d['fields'][key] = params['column']
                        break
                    elif key.startswith(main_include+'.'):
                        d['fields'][key] = params['column']
                else:
                    d['fields']['__data__'] = 'data'

        for include in includes:
            resolve_include(include,collection,include_params)

        for i,include in enumerate(include_list):
            if not include['fields']:
                include['fields']['__data__'] = 'data'
                include['lazy'] = False
                for key,params in self._table_columns[include['collection']].items():
                    if i == 0 and key in excludes:
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

        #we add the order_by_keys seperately
        #(these should not influence whether a document is fetched lazily or not)
        for order_by_key in order_by_keys:
            resolve_include(order_by_key,collection,include_params)

        return include_params

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
        if not '__collection__' in data:
            raise AttributeError("__collection__ attribute not specified!")

        lazy = data['__lazy__']
        collection = data['__collection__']

        if '__data__' in data and data['__data__']:
            d = self.deserialize_json(data['__data__'])
            #We delete excluded key values from the data, so that no poisoning can take place...
            for key in self._excluded_keys[collection]:
                delete_value(d,key)
        else:
            d = {}
        for key,value in data.items():
            if key in ('__data__','__lazy__','__collection__'):
                continue
            set_value(d,key,value)
        return d,lazy

    def create_instance(self, cls_or_collection,attributes,lazy = False):

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection

        #first, we create an object without attributes
        obj = super(Backend,self).create_instance(cls_or_collection, {}, call_hook=False, lazy=lazy, deserialize=False)
        #then, we initialize it with the relationship data
        self.initialize_relations(obj, attributes)
        #then, we deserialize the attributes and assign them to the object
        obj.attributes = self.deserialize(attributes)
        #finally, we call the after_load hook
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
            if len(result) > 1:
                raise cls.MultipleDocumentsReturned
            obj = result[:1][0]
            return obj
        except IndexError:
            raise cls.DoesNotExist


    def filter(self, cls_or_collection, query, raw = False,only = None,include = None):
        """
        Filter objects from the database that correspond to a given set of properties.

        See :py:meth:`blitzdb.backends.base.Backend.filter` for documentation of individual parameters

        .. note::

            This function supports all query operators that are available in SQLAlchemy and returns a query set
            that is based on a SQLAlchemy cursor.

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
                operator = list(query.keys())[0][1:]
                if not operator in ('and','or','not'):
                    raise AttributeError("Non-supported logical operator: $%s" % operator)
                if operator in ('and','or'):
                    where_statements = [sq for expr in query['$%s' % operator] 
                                        for sq in compile_query(collection,expr,path = path)]
                    if operator == 'and':
                        return [and_(*where_statements)]
                    else:
                        return [or_(*where_statements)]
                elif operator  == 'not':
                    return [not_(*compile_query(collection,query['$not'],table = table,path = path))]

            def compile_one_to_many_query(key,query,field_name,related_table,count_column,path):

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
                    return {tail : query_dict}

                tail = key[len(field_name)+1:]

                if isinstance(query,Document) and not tail:
                    query = {'pk' : query.pk}

                #to do: implement $size and $not: {$size} operators...
                if isinstance(query,dict) and len(query) == 1 and list(query.keys())[0] in ('$all','$in','$elemMatch','$nin'):
                    #this is an $in/$all/$nin query
                    query_type = list(query.keys())[0][1:]
                    subquery = list(query.values())[0]

                    if query_type == 'elemMatch':
                        queries = compile_query(params['collection'],
                                                prepare_subquery(tail,query['$elemMatch']),
                                                table = related_table,
                                                path = path)
                        return queries
                    else:
                        if isinstance(subquery,(ManyToManyProxy,QuerySet)):
                            if tail:
                                #this query has a tail
                                query = {tail : query}
                                queries = compile_query(params['collection'],query,
                                                table = related_table,
                                                path = path)
                                return queries
                            #this is a query with a ManyToManyProxy/QuerySet
                            if isinstance(subquery,ManyToManyProxy):
                                qs = subquery.get_queryset()
                            else:
                                qs = subquery
                            if not query_type in ('in','nin','all'):
                                raise AttributeError
                            if query_type == 'all':
                                op = 'in'
                            else:
                                op = query_type
                            if query_type == 'all':
                                cnt = func.count(count_column)
                                condition = cnt == qs.get_count_select()
                                havings.append(condition)
                            return [getattr(related_table.c['pk'],op+'_')(qs.get_select(columns = ['pk']))]
                        elif isinstance(subquery,(list,tuple)):
                            if subquery and isinstance(subquery[0],dict) and len(subquery[0]) == 1 and \
                            list(subquery[0].keys())[0] == '$elemMatch':
                                queries = [sq for v in subquery for sq in compile_query(params['collection'],
                                                                                        prepare_subquery(tail,v['$elemMatch']),
                                                                                        table = related_table,
                                                                                        path = path)]
                            else:
                                queries = [sq for v in subquery for sq in compile_query(params['collection'],
                                                                                        prepare_subquery(tail,v),
                                                                                        table = related_table,
                                                                                        path = path)]
                            where_statement = or_(*queries)

                            if query_type == 'nin':
                                where_statement = not_(where_statement)

                            if query_type == 'all' and len(queries) > 1:
                                cnt = func.count(count_column)
                                havings.append(cnt == len(queries))

                            return [where_statement]
                        else:
                            raise AttributeError("$in/$nin/$all query requires a list/tuple/QuerySet/ManyToManyProxy")
                else:
                    return compile_query(params['collection'],prepare_subquery(tail,query),
                                    table = related_table,
                                    path = path)

            def compile_many_to_many_query(key,value,field_name,related_collection,relationship_table,path):

                related_table = self._collection_tables[related_collection]

                path_str = ".".join(path)

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

            def prepare_special_query(field_name,params,query):
                def sanitize(value):
                    if isinstance(value,(list,tuple)):
                        return [v.pk if isinstance(v,Document) else v for v in value]
                    return value
                column_name = params['column']
                if '$not' in query:
                    return [not_(*prepare_special_query(column_name,params,sanitize(query['$not'])))]
                elif '$in' in query:
                    if not query['$in']:
                        #we return an impossible condition since the $in query does not contain any values
                        return [expression.cast(True,Boolean) == expression.cast(False,Boolean)]
                    return [table.c[column_name].in_(sanitize(query['$in']))]
                elif '$nin' in query:
                    if not query['$nin']:
                        return [expression.cast(True,Boolean) == expression.cast(False,Boolean)]
                    return [~table.c[column_name].in_(sanitize(query['$nin']))]
                elif '$eq' in query:
                    return [table.c[column_name] == sanitize(query['$eq'])]
                elif '$ne' in query:
                    return [table.c[column_name] != sanitize(query['$ne'])]
                elif '$gt' in query:
                    return [table.c[column_name] > sanitize(query['$gt'])]
                elif '$gte' in query:
                    return [table.c[column_name] >= sanitize(query['$gte'])]
                elif '$lt' in query:
                    return [table.c[column_name] < sanitize(query['$lt'])]
                elif '$lte' in query:
                    return [table.c[column_name] <= sanitize(query['$lte'])]
                elif '$exists' in query:
                    if query['$exists']:
                        return [table.c[column_name] != None]
                    else:
                        return [table.c[column_name] == None]
                elif '$like' in query:
                    return [table.c[column_name].like(expression.cast(query['$like'],String))]
                elif '$ilike' in query:
                    return [table.c[column_name].ilike(expression.cast(query['$ilike'],String))]
                elif '$regex' in query:
                    if not self.engine.url.drivername in ('postgres','mysql','sqlite'):
                        raise AttributeError("Regex queries not supported with %s engine!" % self.engine.url.drivername)
                    return [table.c[column_name].op('REGEXP')(expression.cast(query['$regex'],String))]
                else:
                    raise AttributeError("Invalid query!")
            
            #this is a normal, field-base query
            for key,value in query.items():
                for field_name,params in self._index_fields[collection].items():
                    if key == field_name:
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
                    #we check the normal relationships
                    for field_name,params in self._related_fields[collection].items():
                        if key.startswith(field_name):

                            head,tail = key[:len(field_name)],key[len(field_name)+1:]
                            new_path = path + [head]
                            path_str = ".".join(new_path)
                            #ManyToManyField
                            if isinstance(params['field'],ManyToManyField):
                                relationship_table = self._relationship_tables[collection][field_name]
                                where_statements.extend(compile_many_to_many_query(key,value,field_name,params['collection'],relationship_table,path = new_path))
                            elif isinstance(params['field'],ForeignKeyField):#this is a normal ForeignKey relation
                                if key == field_name:
                                    #this is a ForeignKey query
                                    if isinstance(value,dict):
                                        if len(value) == 1:
                                            key,query = list(value.items())[0]
                                            if key == '$exists':
                                                if not isinstance(query,bool):
                                                    raise AttributeError("$exists operator requires a Boolean operator")
                                                if query:
                                                    where_statements.append(table.c[params['column']] != None)
                                                else:
                                                    where_statements.append(table.c[params['column']] == None)
                                                break
                                            elif not key in ('$in','$nin'):
                                                raise AttributeError("Invalid query!")
                                            query_type = key[1:]
                                        else:
                                            raise AttributeError("Invalid query!")
                                    else:
                                        query_type = 'exact'
                                        query = value
                                    if isinstance(query,(QuerySet,ManyToManyProxy)):
                                        if not query_type in ('in','nin'):
                                            raise AttributeError("QuerySet/ManyToManyProxy objects must be used in conjunction with $in/$nin when querying a ForeignKey relationship")
                                        if isinstance(query,ManyToManyProxy):
                                            qs = query.get_queryset()
                                        else:
                                            qs = query
                                        if qs.count is not None and qs.count == 0:
                                            raise AttributeError("$in/$nin query with empty QuerySet/ManyToManyProxy!")
                                        if qs.cls is not params['class']:
                                            raise AttributeError("Invalid QuerySet class!")
                                        condition = getattr(table.c[params['column']],query_type+'_')(qs.get_select(columns = ['pk']))
                                        where_statements.append(condition)
                                    elif isinstance(query,(list,tuple)):
                                        if not query_type in ('in','nin'):
                                            raise AttributeError("Lists/tuples must be used in conjunction with $in/$nin when querying a ForeignKey relationship")
                                        if not query:
                                            raise AttributeError("in/nin query with empty list!")
                                        if query[0].__class__ is params['class']:
                                            if any((element.__class__ is not params['class'] for element in query)):
                                                raise AttributeError("Invalid document type in ForeignKey query")
                                            where_statements.append(getattr(table.c[params['column']],query_type+'_')([expression.cast(doc.pk,params['type']) for doc in query]))
                                        else:
                                            where_statements.append(getattr(table.c[params['column']],query_type+'_')([expression.cast(element,params['type']) for element in query]))
                                    elif isinstance(query,Document):
                                        #we need an exact clas match here...
                                        if query.__class__ is not params['class']:
                                            raise AttributeError("Invalid Document class!")
                                        where_statements.append(table.c[params['column']] == query.pk)
                                    else:
                                        where_statements.append(table.c[params['column']] == expression.cast(query,params['class'].Meta.PkType))
                                else:
                                    #we query a sub-field of the relation
                                    related_table = self._collection_tables[params['collection']]

                                    if path_str in joins[related_table]:
                                        related_table_alias = joins[related_table][path_str]
                                    else:
                                        related_table_alias = related_table.alias()
                                        joins[related_table][path_str] = related_table_alias
                                        joins_list.append((related_table_alias,table.c[params['column']] == related_table_alias.c['pk']))
                                    where_statements.extend(compile_query(params['collection'],{tail : value},table = related_table_alias,path = new_path))
                            elif isinstance(params['field'],OneToManyField):
                                related_table = self._collection_tables[params['collection']]

                                if path_str in joins[related_table]:
                                    related_table_alias = joins[related_table][path_str]
                                else:
                                    related_table_alias = related_table.alias()
                                    joins[related_table][path_str] = related_table_alias
                                    joins_list.append((related_table_alias,related_table_alias.c[params['backref']['column']] == table.c['pk']))

                                where_statements.extend(compile_one_to_many_query(key,value,field_name,related_table_alias,table.c.pk,new_path))
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

        return QuerySet(backend = self, table = table,
                        joins = joins_list,
                        cls = cls,
                        condition = compiled_query,
                        raw = raw,
                        group_bys = group_bys,
                        only = only,
                        include = include,
                        havings = havings
                        )
