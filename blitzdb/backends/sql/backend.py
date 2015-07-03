import abc
import six
import uuid
from collections import defaultdict

from ..document import Document
from ..base import Backend as BaseBackend
from ..base import NotInTransaction
from .queryset import QuerySet

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import MetaData,Table,Column,ForeignKey,UniqueConstraint
from sqlalchemy.types import Integer,VARCHAR,String,Text,LargeBinary,Unicode
from sqlalchemy.sql import select,insert,update,func,and_,or_,not_,expression

"""
Base model for SQL backend:

Data storage:

Define a JSON column in th underlying database

Indexes:

Define additional columns in the table with a given type

M2M-Relationships: Let the user define them through helper documents
"""


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

    class Meta(BaseStore.Meta):

        PkType = VARCHAR(64)

    def __init__(self, engine, table_postfix = '',**kwargs):
        self._engine = engine
        self._collection_tables = {}
        self._index_tables = {}
        self._index_fields = defaultdict(list)
        self.table_postfix = table_postfix
        self.vertex_indexes = self.Meta.vertex_indexes.copy()
        self.edge_indexes = self.Meta.edge_indexes.copy()
        if create_schema:
            self.create_schema()
        self._conn = self._engine.connect()
        super(Backend, self).__init__(**kwargs)

    def init_schema(self):

        self._metadata = MetaData()

        for cls in self.classes:
            collection = self.get_collection_for_cls(cls)

            index_columns = []

            if 'indexes' in meta_attributes:
                for index in meta_attributes['indexes']:
                    index_name = "_".join([field for field in index['fields']])
                    if not 'sql_opts' in index:
                        raise AttributeError("You need to specify a parameter for index %s in class %s" % (index_name,str(cls)))
                    opts = index['sql_opts']
                    try:
                        index_type = opts['type']
                    except KeyError:
                        raise AttributeError("You must specify a type for index %s in class %s" (index_name,str(cls)))
                    if isinstance(opts['type'],six.string_types):
                        if opts['type'] == 'ManyToMany':
                            """
                            This is a ManyToMany index!

                            We create a table for it with two foreign keys and, possibly, a qualifier
                            """
                    else:
                        if len(index['fields']) > 1:
                            field_columns = {}
                            for field_name in index['fields']:
                                field_column_name = 'index_%s_%s' % (index_name,field_name)
                                field_columns.append(
                                    Column(field_column_name,index_type[field_name])
                                    )
                            index_columns.extend(field_columns.values())
                            index_columns.append(Index('index_%s' % index_name,*[name for name in field_columns]))
                        else:
                            self._index_fields[collection].append(index)
                            index_columns.append(
                                Column('index_%s' % index_name,index_type,index = True)
                                )

            self._collection_tables[collection] = Table('vertex%s' % self.table_postfix,self._metadata,
                    Column('pk',self.Meta.PkType,primary_key = True,index = True),
                    Column('data',LargeBinary),
                    *index_columns
                )

            meta_attributes = self.get_meta_attributes(cls)


        def generate_index_tables(metadata,prefix,indexes,foreign_key_column):

            index_tables = {}
            for name,params in indexes.items():
                index_tables[name] = Table('%s_index_%s%s' % (prefix,name,self.table_postfix),metadata,
                    Column(name,params['type'],nullable = True,index = True),
                    Column('pk',self.Meta.PkType,ForeignKey(foreign_key_column),primary_key = False,index = True),
                    UniqueConstraint(name, 'pk', name='%s_%s_pk_unique_%s' % (prefix,name,self.table_postfix) )
                    )
            return index_tables

        self._vertex = Table('vertex%s' % self.table_postfix,self._metadata,
                Column('pk',self.Meta.PkType,primary_key = True,index = True),
                Column('data',LargeBinary),
            )

        self._edge = Table('edge%s' % self.table_postfix,self._metadata,
                Column('pk',self.Meta.PkType,primary_key = True,index = True),
                Column('out_v_pk',self.Meta.PkType,ForeignKey('vertex%s.pk' % self.table_postfix),index = True),
                Column('inc_v_pk',self.Meta.PkType,ForeignKey('vertex%s.pk' % self.table_postfix),index = True),
                Column('label',VARCHAR(128),index = True),
                Column('data',LargeBinary,nullable = True),
            )

        self._vertex_index_tables = generate_index_tables(self._metadata,"vertex",self.vertex_indexes,self._vertex.c.pk)
        self._edge_index_tables = generate_index_tables(self._metadata,"edge",self.edge_indexes,self._edge.c.pk)

    def begin(self):
        return self._conn.begin()

    def commit(self,transaction):
        transaction.commit()

    def rollback(self,transaction):
        transaction.rollback()

    def close_connection(self):
        return self._conn.close()

    def create_schema(self,indexes = None):
        self.init_schema()
        self._metadata.create_all(self._engine,checkfirst = True)

    def drop_schema(self):
        self.init_schema()
        self._metadata.drop_all(self._engine,checkfirst = True)

    def get(self, cls_or_collection, properties):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        #...

    def delete(self, obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            raise obj.DoesNotExist
        #...

    def save(self, obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            obj.pk = uuid.uuid4().hex
        serialized_attributes = self.serialize(obj.attributes)
        serialized_attributes['_id'] = obj.pk
        #...

    def serialize(self, obj, convert_keys_to_str=True, embed_level=0, encoders=None):
        return super(Backend, self).serialize(obj, 
                                              convert_keys_to_str=convert_keys_to_str, 
                                              embed_level=embed_level, 
                                              encoders=encoders)

    def deserialize(self, obj, decoders=None):
        return super(Backend, self).deserialize(obj, decoders=decoders)

    def create_index(self, cls_or_collection, *args, **kwargs):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        self.db[collection].ensure_index(*args, **kwargs)

    def compile_query(self, query):
        if isinstance(query, dict):
            return dict([(self.compile_query(key), self.compile_query(value)) 
                         for key, value in query.items()])
        elif isinstance(query, list):
            return [self.compile_query(x) for x in query]
        else:
            return self.serialize(query)

    def filter(self, cls_or_collection, query, sort_by=None, limit=None, offset=None):
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

        compiled_query = self.compile_query(query)

        return QuerySet(self, cls, self.db[collection].find(compiled_query))
