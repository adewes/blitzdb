import abc
import six
import uuid
import pprint
from collections import defaultdict

from ...document import Document
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

    class Meta(BaseBackend.Meta):

        PkType = VARCHAR(64)

    def __init__(self, engine, table_postfix = '',create_schema = False,**kwargs):
        self._engine = engine
        self._collection_tables = {}

        self._index_tables = {}
        self._relationship_tables = {}
        self._index_fields = defaultdict(dict)
        self._related_fields = defaultdict(dict)

        self.table_postfix = table_postfix

        if create_schema:
            self.create_schema()

        self._conn = self._engine.connect()
        super(Backend, self).__init__(**kwargs)

    def init_schema(self):

        self._metadata = MetaData()

        for cls in self.classes:
            collection = self.get_collection_for_cls(cls)

            index_columns = []

            meta_attributes = self.get_meta_attributes(cls)

            if 'indexes' in meta_attributes:
                for i,index in enumerate(meta_attributes['indexes']):
                    if not 'sql' in index:
                        raise AttributeError("You need to specify a parameter for index %d in class %s" % (i,str(cls)))
                    opts = index['sql']

                    field_name = opts['field']
                    column_name = field_name.replace('.','_')
                    index_name = "%s_%s" % (collection,column_name)

                    self._index_fields[collection][field_name] = {'opts' : index,'column' : column_name}
                    if 'list' in opts:
                        #This is a list index, so we create a dedicated table for it.
                        self._index_tables[index_name] = Table('%s%s' % (index_name,self.table_postfix),self._metadata,
                                Column('pk',self.Meta.PkType,ForeignKey('%s%s.pk' % (collection,self.table_postfix)),index = True),
                                Column(column_name,opts['type']),
                                UniqueConstraint('pk',field_name,name = 'unique_index')
                            )
                    else:
                        index_columns.append(
                            Column(column_name,opts['type'],index = True)
                            )

            if 'relations' in meta_attributes:
                for relation in meta_attributes['relations']:
                    field_name = relation['field']
                    related_collection = self.get_collection_for_cls_name(relation['related'])
                    if 'qualifier' in relation:
                        relationship_name = "%s_%s_%s" % (collection,related_collection,relation['qualifier'])
                    else:
                        relationship_name = "%s_%s" % (collection,related_collection)
                    if relation['type'] == 'ManyToMany':
                        self._related_fields[collection][field_name] = {'opts' : relation,
                                                                        'collection' : related_collection,
                                                                        'relationship_table' : relationship_name,
                                                                        }
                        if isinstance(relation['related'],(list,tuple)):
                            raise AttributeError("Currently not supported!")
                            #this is a relation to multiple collections
                        else:
                            #this is a relation with one single collection
                            extra_columns = []
                            if 'qualifier' in relation:
                                extra_columns = [
                                    Column(relation['qualifier'],String,index = True),
                                    UniqueConstraint('pk_%s' % related_collection,'pk_%s' % collection,relation['qualifier'])
                                ]
                            else:
                                extra_columns = [
                                    UniqueConstraint('pk_%s' % related_collection,'pk_%s' % collection)
                                    ]
                            self._relationship_tables[relationship_name] = Table('%s%s' % (relationship_name,self.table_postfix),self._metadata,
                                    Column('pk_%s' % related_collection,self.Meta.PkType,ForeignKey('%s%s.pk' % (related_collection,self.table_postfix)),index = True),
                                    Column('pk_%s' % collection,self.Meta.PkType,ForeignKey('%s%s.pk' % (collection,self.table_postfix)),index = True),
                                    *extra_columns
                                )
                    elif relation['type'] == 'ForeignKey':
                        column_name = relation['field'].replace('.','_')
                        self._related_fields[collection][field_name] = {'opts' : relation,
                                                                        'column' : column_name,
                                                                        'collection' : related_collection,
                                                                        }
                        index_columns.append(
                            Column(column_name,self.Meta.PkType,ForeignKey('%s%s.pk' %(related_collection,self.table_postfix)),index=True,nullable = True if 'sparse' in relation and relation['sparse'] else False)
                            )

            self._collection_tables[collection] = Table('%s%s' % (collection,self.table_postfix),self._metadata,
                    Column('pk',self.Meta.PkType,primary_key = True,index = True),
                    Column('data',LargeBinary),
                    *index_columns
                )

        pprint.pprint(self._collection_tables)
        pprint.pprint(self._index_tables)
        pprint.pprint(self._relationship_tables)


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


    def delete_vertex(self,vertex):
        try:
            trans = self.begin()
            delete_edges = self._edge.delete().where(self._edge.c.inc_v_pk == vertex.pk or self._edge.c.out_v_pk == vertex.pk)
            self._conn.execute(delete_edges)
            self._remove_vertex_from_index(vertex.pk)
            delete_vertex = self._vertex.delete().where(self._vertex.c.pk == vertex.pk)
            self._conn.execute(delete_vertex)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def delete_edge(self,edge):
        try:
            trans = self.begin()
            self._remove_edge_from_index(edge.pk)
            delete_edge = self._edge.delete().where(self._edge.c.pk == edge.pk)
            self._conn.execute(delete_edge)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def update_vertex(self,vertex):
        if not isinstance(vertex,self.Meta.Vertex):
            raise TypeError("Must be a vertex!")
        serialized_data = self.serialize_vertex_data(vertex.data_rw)

        try:
            trans = self.begin()
            update = self._vertex.update().values(**serialized_data)\
                                          .where(self._vertex.c.pk == expression.cast(vertex.pk,self.Meta.PkType))
            self._conn.execute(update)
            self._remove_vertex_from_index(vertex.pk)
            self._add_to_index(vertex.pk,vertex.data_rw,self._vertex_index_tables)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise
        return vertex

    def serialize_vertex_data(self,data):
        d = {'data' : cPickle.dumps(data)}
        if 'pk' in data:
            d['pk'] = data['pk']
        return d

    def serialize_edge_data(self,data):
        return cPickle.dumps(data)

    def _add_to_index(self,pk,data,index_tables):

        def add_to_index(key,table):
            if not key in data:
                return
            d = {'pk' : pk,key : data[key]}
            insert = table.insert().values(**d)
            self._conn.execute(insert)
        try:
            trans = self.begin()
            for key,table in index_tables.items():
                add_to_index(key,table)
            self.commit(trans)
        except IntegrityError:
            self.rollback(trans)
            raise

    def _remove_vertex_from_index(self,pk):
        try:
            trans = self.begin()
            for key,table in self._vertex_index_tables.items():
                delete = table.delete().where(table.c['pk'] == str(pk))
                self._conn.execute(delete)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def _remove_edge_from_index(self,pk):
        try:
            trans = self.begin()
            for key,table in self._edge_index_tables.items():
                delete = table.delete().where(table.c['pk'] == str(pk))
                self._conn.execute(delete)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise

    def save(self,obj):
        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            obj.pk = uuid.uuid4().hex

        serialized_attributes = self.serialize(obj.attributes)

        insert = self._collection_tables.insert().values(**serialized_attributes)
        trans = self.begin()
        try:
            self._conn.execute(insert)
            self._add_to_index(d['pk'],data,self._vertex_index_tables)
            self.commit(trans)
        except:
            self.rollback(trans)
            raise
        return self.Meta.Vertex(d['pk'],store = self,db_data = data)

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

        def compile_query(collection,query):

            table = self._collection_tables[collection]

            where_statements  = []

            if len(query) == 1 and query.keys()[0].startswith('$'):
                #this is a special operator query
                operator = query.keys()[0][1:]
                if not operator in ('and','or','not'):
                    raise AttributeError("Non-supported logical operator: $%s" % operator)
                if operator in ('and','or'):
                    where_statements = [compile_query(collection,expression) for expression in query['$and']]
                    if operator == 'and':
                        return and_(where_statements)
                    return or_(where_statements)
                elif operator  == 'not':
                    return not_(compile_query(query['$not']))

            """
                    #Valid query operators for list index / M2M field
                    '$all': all_query,
                    '$elemMatch': elemMatch_query,
                    '$in': in_query,
                     $size

                    #Valid operators for index fields:
                    '$exists': exists_query,
                    '$gte': comparison_operator_query(operator.ge),
                    '$lte': comparison_operator_query(operator.le),
                    '$gt': comparison_operator_query(operator.gt),
                    '$lt': comparison_operator_query(operator.lt),
                    '$ne': comparison_operator_query(operator.ne),
                    '$not': not_query,
                    '$in': in_query,
            """

            #this is a normal, field-base query
            for key,value in query.items():

                for field_name,opts in self._index_fields.items():
                    if  key == field_name:
                        #this is an indexed list field!
                        if 'list' in opts and opts['list']:
                            if isinstance(value,dict):
                                #special query
                                pass
                            else:
                                pass
                                #normal value query
                        else:
                            if isinstance(value,dict):
                                #this is a special query
                                pass
                            else:
                                #this is a normal value query
                                pass
                else:
                    #this is a non-indexed field! We try to find a relation...
                    for field_name,opts in self._related_fields.items():
                        if key.startswith(field_name):
                            if opts['opts']['type'] == 'ManyToMany':
                                if key == field_name:

                                    if isinstance(value,dict):
                                        if len(value) == 1 and value.keys()[0].startswith('$'):
                                            operator = value.keys()[0][1:]
                                            if operator not in ('$all','$elemMatch','$in','$size'):
                                                raise AttributeError
                                            if operator == '$all':
                                                pass
                                            elif operator == '$in':
                                                elements = [element.pk for element in value.values()[0]]
                                                where_statements.append(
                                                    )
                                            elif operator == '$size':
                                                pass
                                        else:
                                            pass
                                            #this is an exact query
                                    else:
                                        pass
                                elif key == field_name+'.pk':
                                    pass
                                else:
                                    """
                                    We query a deep field of the related collection
                                    """
                                    pk_field = self._relationship_tables[opts['relationship_table']].c['pk_%s' % collection]
                                    where_statements.append(table.c.pk._in(
                                        select(
                                            [pk_field]
                                            ).where(
                                                
                                                pk_field.in_(compile_query(opts['collection'],{tail : value}))
                                            )
                                        ))
                            else:#this is a normal ForeignKey relation
                                if key == field_name:
                                    where_statements.append(compile_column_query(table.c[opts['column']],value))
                                elif key == field_name+'.pk':
                                    pass
                                else:
                                    #we query a sub-field of the relation
                                    head,tail = key[:len(field_name)],key[len(field_name)+1:]
                                    where_statements.append(table.c.pk.in_(compile_query(opts['collection'],{tail : value})))
                    else:
                        raise AttributeError("Query over non-indexed field %s!")
            return

        compiled_query = compile_query(collection,query)

        return QuerySet(self, cls, self.db[collection].find(compiled_query))
