import abc
import six
import uuid
from collections import defaultdict

from blitzdb.document import Document
from blitzdb.backends.base import Backend as BaseBackend
from blitzdb.backends.base import NotInTransaction

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import MetaData,Table,Column,ForeignKey,UniqueConstraint
from sqlalchemy.types import Integer,String,Text,LargeBinary,Unicode
from sqlalchemy.sql import select,insert,update,func,and_,or_,not_,in_,expression

from .relations import ManyToMany,ForeignKey as OurForeignKey

"""
Base model for SQL backend:

M2M-Relationships: Let the user define them through helper documents
"""


class Backend(BaseBackend):

    """
    A SQL backend.

    :param db: An instance of a `sqlalchemy. 
    <http://www.sqlalchemy.org>`_ class

    Example usage:

    .. code-block:: python

        from sqlalchemy import
        from blitzdb.backends.sql import Backend as SQLBackend

        my_db = ...

        #create a new BlitzDB backend using a SQLAlchemy database
        backend = SQLBackend(my_db)
    """

    class Meta(BaseBackend.Meta):

        table_postfix = ''
        PkType = String

    def __init__(self, engine, create_schema = True,table_postfix = None,**kwargs):
        self._engine = engine

        self.table_postfix = table_postfix if table_postfix is not None else self.Meta.table_postfix

        super(Backend, self).__init__(**kwargs)

        self.init_schema()
        if create_schema:
            self.create_schema()
        self._conn = self._engine.connect()

    def create_schema(self):
        self.init_schema()
        self._metadata.create_all(self._engine,checkfirst = True)

    def create_index(self,key,params,initialize = True,overwrite = False):
        if key in self.vertex_indexes and not overwrite:
            return
        
        self.vertex_indexes[key] = params

        self.init_schema()
        self.create_schema()

        index_tables = {key : self._vertex_index_tables[key]}
        if initialize:
            trans = self.begin()
            for vertex in self.filter({}):
                self._add_to_index(vertex.pk,vertex.data_rw,index_tables)
            self.commit(trans)

    def _add_to_index(self,pk,data,index_tables):

        def add_to_index(key,table):
            if not key in data:
                return
            d = {'pk' : pk,key : data[key]}
            insert = table.insert().values(**d)
            try:
                trans = self.begin()
                self._conn.execute(insert)
                self.commit(trans)
            except IntegrityError:
                self.rollback(trans)

        for key,table in index_tables.items():
            add_to_index(key,table)

    def init_schema(self):

        self._metadata = MetaData()

        self._tables = {}
        self._index_fields = defaultdict(dict)
        self._foreign_key_fields = defaultdict(dict)
        self._many_to_many_tables = defaultdict(dict)

        for name,cls in self.collections.items():

            foreign_key_columns = []
            index_columns = []

            if hasattr(cls,'Meta'):
                if hasattr(cls.Meta,'indexes'):
                    for field,params in cls.Meta.indexes.items():
                        index_columns.append(
                            Column(field,params['sql_type'],index = True,nullable = True)
                            )
                    self._index_fields[name][field] = params
                if hasattr(cls.Meta,'foreign_key_fields'):
                    for field,params in cls.Meta.foreign_key_fields.items():

                        if issubclass(params['to'],Document):
                            to_cls = params['to']
                        else:
                            to_cls = self.get_cls_for_collection(params['to'])

                        self._foreign_key_fields[name][field] = params

                        collection = self.get_collection_for_cls(to_cls)
                        setattr(cls,field,OurForeignKey(to_cls))
                        foreign_key_columns.append(Column('pk_%s' % field,
                            self.Meta.PkType,ForeignKey("%s%s.pk" % (collection,self.table_postfix))))
                if hasattr(cls.Meta,'many_to_many_fields'):
                    for field,params in cls.Meta.many_to_many_fields.items():

                        if issubclass(params['to'],Document):
                            to_cls = params['to']
                        else:
                            to_cls = self.get_cls_for_collection(params['to'])

                        collection = self.get_collection_for_cls(to_cls)

                        setattr(cls,field,ManyToMany(params['to']))

                        self._many_to_many_tables[name][field] = Table(
                            '%s_m2m_%s_%s%s' % (name,collection,field,self.Meta.table_postfix),
                            self._metadata,
                            Column('pk_%s' % (name),self.Meta.PkType,ForeignKey("%s%s.pk" % (name,self.table_postfix))),
                            Column('pk_%s' % (collection),self.Meta.PkType,ForeignKey("%s%s.pk" % (collection,self.table_postfix))),
                            Column('qualifier',String),
                            UniqueConstraint('pk_%s' % name,'pk_%s' % collection,'qualifier'),
                            )

            self._tables[name] = Table('%s%s' % (name,self.table_postfix),self._metadata,
                    Column('pk',self.Meta.PkType,primary_key = True,index = True),
                    Column('data',LargeBinary),
                    *(foreign_key_columns+index_columns)
                )

    def begin(self):
        pass

    def rollback(self):
        raise NotInTransaction("SQLAlchemy backend does not support rollback!")

    def commit(self):
        pass

    def get(self, cls_or_collection, query):
        pass

    def delete(self, obj):
        collection = self.get_collection_for_cls(obj.__class__)
        pass

    def save(self, obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            obj.pk = uuid.uuid4().hex
        pass

    def serialize(self, obj, convert_keys_to_str=True, embed_level=0, encoders=None):
        return super(Backend, self).serialize(obj, 
                                              convert_keys_to_str=convert_keys_to_str, 
                                              embed_level=embed_level, 
                                              encoders=encoders)

    def deserialize(self, obj, decoders=None):
        return super(Backend, self).deserialize(obj, decoders=decoders)

    def filter(self, cls_or_collection, query, sort_by=None, limit=None, offset=None):
        """
        """

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        index_fields = self._index_fields[collection]
        foreign_key_fields = self._foreign_key_fields[collection]
        many_to_many_fields = self._many_to_many_tables[collection]
        table = self._tables[collection]

        def compile_query(query,parent_key = None):
            and_expressions = []
            special_op = None
            if not isinstance(query,dict):
                return query
            for key in query.keys():
                if key.startswith('$'):
                    special_op = key
                    break
            if special_op:
                sq = query[special_op]
                if special_op == 'exists':
                    return compile_query(query[special_op]) is not None
                elif special_op == '$and':
                    return and_([compile_query(q) for q in sq])
                elif special_op == '$or':
                    return or_([compile_query(q) for q in sq])
                elif special_op == '$in':
                    return in([q for q in sq])
                elif special_op == '$not':
                    return not_(compile_query(sq))
                elif special_op == '$gte':
                    return value_query(parent_key,sq,'gte')
                elif special_op == '$gt':
                    return value_query(parent_key,sq,'gt')
                elif special_op == '$lte':
                    return value_query(parent_key,sq,'lte')
                elif special_op == '$lt':
                    return value_query(parent_key,sq,'lt')
                elif special_op == '$ne':
                    return value_query(parent_key,sq,'ne')
                else:
                    raise AttributeError("Unsupported special operator: %s" % special_op)
            else:
                for key,value in query.items():
                    and_expressions.append(value_query(key,compile_query(value,key),'eq'))

        def value_query(key,value,op = 'eq'):

            def comparator(a,b):
                if op == 'eq':
                    return a == b
                elif op == 'ne':
                    return a != b
                elif op == 'lt':
                    return a < b
                elif op == 'lte':
                    return a <= b
                elif op == 'gt':
                    return a > b
                elif opt == 'gte':
                    return a >= b
                raise AttributeError("Invalid comparison operator: %s" % op)

            compiled_value = compile_query(value)

            if key in index_fields or key == 'pk':
                return comparator(table.c[key],compiled_value)
            elif key in foreign_key_fields:
                #this is a foreign key query
                if isinstance(compiled_value,Document):
                    return comparator(table.c[key],compiled_value.pk)
                else:
                    return comparator(table.c[key],compiled_value)
            raise AttributeError('Query over non-indexed field: %s' % key)

        compiled_query = compile_query(query)

        queryset = QuerySet(self,table,self._conn,condition = compiled_query,
                            deserializer = deserializer)

        return queryset
