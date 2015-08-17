import abc
import six
import uuid
import pprint
from collections import defaultdict

from ...document import Document
from ..base import Backend as BaseBackend
from ..base import NotInTransaction,DoNotSerialize
from ..file.serializers import JsonSerializer
from .queryset import QuerySet

from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import MetaData,Table,Column,ForeignKey,UniqueConstraint
from sqlalchemy.types import Integer,VARCHAR,String,Text,LargeBinary,Unicode
from sqlalchemy.sql import select,insert,update,func,and_,or_,not_,expression,null

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
        super(Backend, self).__init__(**kwargs)

        self._engine = engine
        self._collection_tables = {}

        self._index_tables = defaultdict(dict)
        self._relationship_tables = defaultdict(dict)
        self._index_fields = defaultdict(dict)
        self._related_fields = defaultdict(dict)
        self._excluded_fields = defaultdict(dict)
        self._transaction = None

        self.table_postfix = table_postfix

        if create_schema:
            self.create_schema()

        self._conn = self._engine.connect()

        self.begin()

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

                    if callable(opts):
                        opts = opts()

                    field_name = opts['field']
                    self._excluded_fields[collection][field_name] = True
                    column_name = field_name.replace('.','_')
                    index_name = "%s_%s" % (collection,column_name)

                    index_params = {'opts' : opts,
                                    'column' : column_name}
                    if 'list' in opts and opts['list']:
                        #This is a list index, so we create a dedicated table for it.
                        self._index_tables[collection][field_name] = Table('%s%s' % (index_name,self.table_postfix),self._metadata,
                                Column('pk',self.Meta.PkType,ForeignKey('%s%s.pk' % (collection,self.table_postfix)),index = True),
                                Column(column_name,opts['type'],index = True),
                                UniqueConstraint('pk',field_name,name = 'unique_index')
                            )
                    else:
                        index_columns.append(
                            Column(column_name,opts['type'],index = True)
                            )
                    self._index_fields[collection][field_name] = index_params

            if 'relations' in meta_attributes:
                for relation in meta_attributes['relations']:
                    field_name = relation['field']
                    self._excluded_fields[collection][field_name] = True
                    related_collection = self.get_collection_for_cls_name(relation['related'])
                    if 'qualifier' in relation:
                        relationship_name = "%s_%s_%s" % (collection,related_collection,relation['qualifier'])
                    else:
                        relationship_name = "%s_%s" % (collection,related_collection)
                    if relation['type'] == 'ManyToMany':
                        self._related_fields[collection][field_name] = {'opts' : relation,
                                                                        'collection' : related_collection,
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
                            self._relationship_tables[collection][field_name] = Table('%s%s' % (relationship_name,self.table_postfix),self._metadata,
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
                            Column(column_name,self.Meta.PkType,ForeignKey('%s%s.pk' %(related_collection,self.table_postfix)),index=True,nullable = True if 'nullable' in relation and relation['nullable'] else False)
                            )
                    else:
                        raise AttributeError("Unknown relationship type %s" % relation['type'])

            self._collection_tables[collection] = Table('%s%s' % (collection,self.table_postfix),self._metadata,
                    Column('pk',self.Meta.PkType,primary_key = True,index = True),
                    Column('data',LargeBinary),
                    *index_columns
                )

    def begin(self):
        if self._transaction:
            self.commit()
        self._transaction = self._conn.begin()

    def commit(self):
        self._transaction.commit()
        self._transaction = None
        self.begin()

    def rollback(self):
        self._transaction.rollback()
        self._transaction = None
        self.begin()

    def close_connection(self):
        return self._conn.close()

    def create_schema(self,indexes = None):
        self.init_schema()
        self._metadata.create_all(self._engine,checkfirst = True)

    def drop_schema(self):
        self.init_schema()
        self._metadata.drop_all(self._engine,checkfirst = True)

    def delete(self, obj):

        if hasattr(obj, 'pre_delete') and callable(obj.pre_delete):
            obj.pre_delete()

        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            raise obj.DoesNotExist
        #...

    def save(self,obj,autosave_dependent = True):
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

        if hasattr(obj, 'pre_save') and callable(obj.pre_save):
            obj.pre_save()

        def get_value(obj,key):
            key_fragments = key.split(".")
            current_dict = obj
            for key_fragment in key_fragments:
                current_dict = current_dict[key_fragment]
            return current_dict

        def serialize_and_update_indexes(obj,d):
            for index_field,index_params in self._index_fields[collection].items():
                try:
                    value = get_value(obj,index_field)
                    if 'list' in index_params['opts'] and index_params['opts']['list']:
                        table = self._index_tables[collection][index_field]
                        delete = table.delete().where(table.c['pk'] == expression.cast(obj.pk,self.Meta.PkType))
                        self._conn.execute(delete)
                        for element in value:
                            ed = {
                                'pk' : expression.cast(obj.pk,self.Meta.PkType),
                                index_params['column'] : expression.cast(element,index_params['opts']['type']),
                            }
                            insert = table.insert().values(**ed)
                            self._conn.execute(insert)
                    else:
                        if value is None:
                            if not 'nullable' in index_params['opts'] or not index_params['opts']['nullable']:
                                raise ValueError("No value for %s given, but this is a mandatory field!" % index_field)
                            d[index_params['column']] = null()
                        else:
                            d[index_params['column']] = expression.cast(value,index_params['opts']['type'])
                except KeyError:
                    if not 'list' in index_params['opts']:
                        if not 'nullable' in index_params['opts'] or not index_params['opts']['nullable']:
                            raise ValueError("No value for %s given, but this is a mandatory field!" % index_field)
                        d[index_params['column']] = null()

        def serialize_and_update_relations(obj,d):
            for related_field,relation_params in self._related_fields[collection].items():
                try:
                    value = get_value(obj,related_field)
                    if relation_params['opts']['type'] == 'ManyToMany':
                        relationship_table = self._relationship_tables[collection][related_field]
                        #implement this...
                        delete = relationship_table.delete().where(relationship_table.c['pk_%s' % collection] == expression.cast(obj.pk,self.Meta.PkType))
                        self._conn.execute(delete)
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
                            insert = relationship_table.insert().values(**ed)
                            self._conn.execute(insert)
                    elif relation_params['opts']['type'] == 'ForeignKey':
                        if not isinstance(value,Document):
                            raise AttributeError("Field %s must be a document!" % related_field)
                        if value.pk is None:
                            if autosave_dependent:
                                self.save(value)
                            else:
                                raise AttributeError("Related document in field %s has no primary key!" % related_field)
                        d[relation_params['column']] = expression.cast(value.pk,self.Meta.PkType)

                except KeyError:
                    #this index value does not exist in the object
                    pass

        insert = False

        if obj.pk is None:
            obj.pk = uuid.uuid4().hex
            insert = True

        d = {'data' : JsonSerializer.serialize(self.serialize(obj.attributes)),
             'pk' : expression.cast(obj.pk,self.Meta.PkType)}

        serialize_and_update_indexes(obj,d)
        serialize_and_update_relations(obj,d)

        if not insert:
            try:
                update = self._collection_tables[collection].update().values(**d).where(table.c.pk == obj.pk)
                self._conn.execute(update)
            except:
                #this document does not exist in the DB yet, we try to insert it instead...
                insert = True
        if insert:
            insert = self._collection_tables[collection].insert().values(**d)
            self._conn.execute(insert)

        return obj

    def serialize(self, obj, convert_keys_to_str=True, embed_level=0, encoders=None,**kwargs):
        """
        Serialization strategy:
        """
        
        return super(Backend, self).serialize(obj,
                                              convert_keys_to_str=convert_keys_to_str, 
                                              embed_level=embed_level,
                                              **kwargs)

    def deserialize(self, obj, encoders=None):
        return super(Backend, self).deserialize(obj,encoders = encoders)

    def create_index(self, cls_or_collection, *args, **kwargs):
        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        self.db[collection].ensure_index(*args, **kwargs)

    def get(self, cls_or_collection, query):

        if not isinstance(cls_or_collection, six.string_types):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        result = self.filter(cls_or_collection,query)
        try:
            return result[0]
        except IndexError:
            raise cls.DoesNotExist


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

        table = self._collection_tables[collection]

        def compile_query(collection,query):

            """
            This function emits a list of WHERE statements that can be used to retrieve 
            """

            table = self._collection_tables[collection]

            where_statements  = []

            if len(query) == 1 and query.keys()[0].startswith('$'):
                #this is a special operator query
                operator = query.keys()[0][1:]
                if not operator in ('and','or','not'):
                    raise AttributeError("Non-supported logical operator: $%s" % operator)
                if operator in ('and','or'):
                    where_statements = [sq for expr in query['$%s' % operator] for sq in compile_query(collection,expr)]
                    print where_statements
                    if operator == 'and':
                        return [and_(*where_statements)]
                    else:
                        return [or_(*where_statements)]
                elif operator  == 'not':
                    return not_(compile_query(query['$not']))

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

            def prepare_special_query(field_name,query):
                if '$not' in query:
                    return [not_(*prepare_special_query(field_name,query['$not']))]
                elif '$in' in query:
                    return [table.c[field_name].in_(query['$in'])]
                elif '$nin' in query:
                    return [~table.c[field_name].in_(query['$in'])]
                elif '$eq' in query:
                    return [table.c[field_name] == query['$eq']]
                elif '$ne' in query:
                    return [table.c[field_name] != query['$ne']]
                elif '$gt' in query:
                    return [table.c[field_name] > query['$gt']]
                elif '$gte' in query:
                    return [table.c[field_name] >= query['$gte']]
                elif '$lt' in query:
                    return [table.c[field_name] < query['$lt']]
                elif '$lte' in query:
                    return [table.c[field_name] <= query['$lte']]
                elif '$exists' in query:
                    if query['$exists']:
                        return [table.c[field_name] != None]
                    else:
                        return [table.c[field_name] == None]
                elif '$like' in query:
                    where_statements.append(table.c[field_name].like(expression.cast(query['$regex'],String)))
                elif '$regex' in query:
                    if not self._engine.url.drivername in ('postgres','mysql'):
                        raise AttributeError("Regex queries not supported with %s engine!" % self._engine.url.drivername)
                    where_statements.append(table.c[field_name].op('REGEXP')(expression.cast(query['$regex'],String)))
                else:
                    raise AttributeError("Invalid query!")

            #this is a normal, field-base query
            for key,value in query.items():
                if key == 'pk':
                    where_statements.append(table.c.pk == expression.cast(value,self.Meta.PkType))
                    continue
                for field_name,params in self._index_fields[collection].items():
                    if key == field_name:
                        #this is a list-indexed field
                        """
                        WHERE ...
                            LEFT JOIN [...] ON [...]
                        """
                        if 'list' in params['opts'] and params['opts']['list']:
                            index_table = self._index_tables[collection][field_name]
                            if isinstance(value,dict):
                                related_query = lambda op: index_table.c[field_name].in_([expression.cast(v,params['opts']['type']) for v in value[op]])
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
                                where_statements.append(index_table.c[field_name] == expression.cast(value,params['opts']['type']))
                        else:
                            #this is a normal column index
                            if isinstance(value,dict):
                                #this is a special query
                                where_statements.extend(prepare_special_query(field_name,value))
                            else:
                                #this is a normal value query
                                where_statements.append(table.c[field_name] == expression.cast(value,params['opts']['type']))
                        break
                else:
                    #this is a non-indexed field! We try to find a relation...
                    for field_name,params in self._related_fields[collection].items():
                        if key.startswith(field_name):
                            if params['opts']['type'] == 'ManyToMany':
                                relationship_table = self._relationship_tables[collection][field_name]
                                related_collection = params['collection']
                                related_table = self._collection_tables[related_collection]
                                tail = key[len(field_name)+1:]
                                if not isinstance(value,dict):
                                    if tail:
                                        value = {tail : value}
                                    else:
                                        raise AttributeError("Query over a ManyToMany field must be a dictionary!")
                                #to do: allow modifiers when using special queries (e.g. for regex)
                                #Currently we only support $elemMatch, $all and $in operators
                                if len(value) == 1 and value.keys()[0].startswith('$'):
                                    operator = value.keys()[0][1:]
                                    subquery = value.values()[0]
                                    if operator == 'elemMatch':
                                        query_type = 'all'
                                        queries = compile_query(params['collection'],prepare_subquery(tail,value['$elemMatch']))
                                    elif operator == 'all':
                                        query_type = 'all'
                                        if len(subquery) and isinstance(subquery[0],dict) and len(subquery[0]) == 1 and \
                                        subquery[0].keys()[0] == '$elemMatch':
                                            queries = [sq for v in subquery for sq in compile_query(params['collection'],prepare_subquery(tail,v['$elemMatch']))]
                                        else:
                                            queries = [sq for v in subquery for sq in compile_query(params['collection'],prepare_subquery(tail,v))]
                                    elif operator == 'in':
                                        query_type = 'in'
                                        queries = [sq for v in subquery for sq in compile_query(params['collection'],prepare_subquery(tail,v))]
                                    elif operator == 'nin':
                                        query_type = 'nin'
                                        queries = [sq for v in subquery for sq in compile_query(params['collection'],prepare_subquery(tail,v))]
                                    elif operator == '$size':
                                        raise AttributeError("Size operator is currently not supported!")
                                    else:
                                        raise AttributeError("Unsupported operator: %s" % operator)
                                else:
                                    query_type = 'all'
                                    queries = compile_query(params['collection'],value)
                                    #this is an exact query
                                related_select = select([related_table.c.pk]).where(or_(*queries))
                                related_query = relationship_table.c['pk_%s' % related_collection].in_(related_select)
                                pk_column = relationship_table.c['pk_%s' % collection].label('pk')
                                if query_type == 'all':
                                    cnt = func.count(pk_column).label('cnt')
                                    s = select([pk_column]).where(related_query).group_by('pk').having(cnt == len(queries))
                                elif query_type == 'in':
                                    s = select([pk_column]).where(related_query)
                                elif query_type == 'nin':
                                    s = select([pk_column]).where(not_(related_query))
                                else:
                                    raise AttributeError("Invalid query!")
                                where_statements.append(table.c.pk.in_(s))
                            else:#this is a normal ForeignKey relation
                                if key == field_name:
                                    if not isinstance(value,Document):
                                        raise AttributeError("ForeignKey query with non-document!")
                                    where_statements.append(table.c[params['column']] == value.pk)
                                elif key == field_name+'.pk':
                                    pass
                                else:
                                    #we query a sub-field of the relation
                                    head,tail = key[:len(field_name)],key[len(field_name)+1:]
                                    where_statements.append(table.c.pk.in_(compile_query(params['collection'],{tail : value})))
                            print "Broke!"
                            break
                    else:
                        raise AttributeError("Query over non-indexed field %s!" % key)
            return where_statements

        compiled_query = compile_query(collection,query)

        if len(compiled_query) > 1:
            compiled_query = and_(*compiled_query)
        elif compiled_query:
            compiled_query = compiled_query[0]
        else:
            compiled_query = None

        return QuerySet(backend = self, table = table,cls = cls,connection = self._conn,
                        condition = compiled_query)
