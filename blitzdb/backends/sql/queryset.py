import time
import copy
import sqlalchemy
import pprint

from blitzdb.queryset import QuerySet as BaseQuerySet
from functools import wraps
from sqlalchemy.sql import select,func,expression,delete,distinct,and_,union,intersect
from sqlalchemy.sql.expression import join,asc,desc,outerjoin
from ..file.serializers import JsonSerializer
from .helpers import set_value

class QuerySet(BaseQuerySet):

    def __init__(self, backend, table, cls,
                 condition = None,
                 select = None,
                 intersects = None,
                 raw = False,
                 include = None,
                 only = None,
                 joins = None,
                 extra_fields = None,
                 group_bys = None,
                 objects = None,
                 havings = None,
                 limit = None,
                 offset = None
                 ):
        super(QuerySet,self).__init__(backend = backend,cls = cls)

        self.joins = joins
        self.backend = backend
        self.condition = condition
        self.select = select
        self.havings = havings
        self.only = only
        self.include = include
        self.extra_fields = extra_fields
        self.group_bys = group_bys
        self.cls = cls
        self._limit = limit
        self._offset = offset
        self.table = table
        self._raw = raw
        self.intersects = intersects
        self.objects = objects
        if self.objects:
            self.pop_objects = self.objects[:]

        self._it = None
        self.order_bys = None
        self.count = None
        self.result = None

    def limit(self,limit):
        self._limit = limit
        return self

    def offset(self,offset):
        self._offset = offset
        return self

    def deserialize(self, data):
        if '__lazy__' in data:
            lazy = data['__lazy__']
        else:
            lazy = False
        if '__data__' in data:
            d = self.backend.deserialize_json(data['__data__'])
        else:
            d = {}
        for key,value in data.items():
            if key in ('__data__','__lazy__'):
                continue
            set_value(d,key,value)

        pprint.pprint(d)

        if self._raw:
            #we map the retrieved fields back to their original position in the document...
            return d


        deserialized_attributes = self.backend.deserialize(d)
        obj = self.backend.create_instance(self.cls, deserialized_attributes,lazy = lazy)

        return obj

    def sort(self, keys,direction = None):
        #we sort by a single argument
        if direction:
            keys = ((keys,direction),)
        order_bys = []
        for key,direction in keys:
            if direction > 0:
                direction = asc
            else:
                direction = desc
            try:
                column = self.backend.get_column_for_key(self.cls,key)
            except KeyError:
                raise AttributeError("Attempting to sort results by a non-indexed field %s" % key)
            order_bys.append(direction(column))
        self.order_bys = order_bys
        self.objects = None
        return self

    def next(self):
        if self._it is None:
            self._it = iter(self)
        return self._it.next()

    __next__ = next

    def __iter__(self):
        if self.objects is None:
            self.get_objects()
        for obj in self.objects:
            yield self.deserialize(obj)
        raise StopIteration

    def __contains__(self, obj):
        pks = self.distinct_pks()
        if isinstance(obj, list) or isinstance(obj, tuple):
            obj_list = obj
        else:
            obj_list = [obj]
        for obj in obj_list:
            if obj.pk not in pks:
                return False
        return True

    def get_objects(self):

        s = self.get_select()

        #We create a CTE, which will allow us to join the required includes.
        s_cte = s.cte("results")
        rows = []
        joins = []
        keymap = {}

        def join_table(collection,table,key,params,path = None):
            if path is None:
                path = []
            if 'relationship_table' in params['relation']:
                join_many_to_many(collection,table,key,params,path)
            else:
                join_foreign_key(collection,table,key,params,path)

        def process_fields_and_subkeys(related_collection,related_table,params,path):

            params['table_fields'] = {}
            for field,column_name in params['fields'].items():
                column_label = '_'.join(path+[column_name])
                params['table_fields'][field] = column_label
                rows.append(related_table.c[column_name].label(column_label))

            for subkey,subparams in params['joins'].items():
                join_table(params['collection'],related_table,subkey,subparams,path = path)

        def join_foreign_key(collection,table,key,params,path):
            related_table = params['table'].alias()
            related_collection = params['relation']['collection']
            condition = table.c[params['relation']['column']] == related_table.c.pk
            joins.append((related_table,condition))
            process_fields_and_subkeys(related_collection,related_table,params,path+\
                                        [params['relation']['column']])

        def join_many_to_many(collection,table,key,params,path):
            relationship_table = params['relation']['relationship_table'].alias()
            related_collection = params['relation']['collection']
            related_table = self.backend.get_collection_table(related_collection).alias()
            left_condition = relationship_table.c['pk_%s' % collection] == table.c.pk
            right_condition = relationship_table.c['pk_%s' % related_collection] == related_table.c.pk
            joins.append((relationship_table,left_condition))
            joins.append((related_table,right_condition))
            process_fields_and_subkeys(related_collection,related_table,params,path+[key])

        def unpack_many_to_many(objects,params,pk_key,pk_value):
            """
            We unpack a many-to-many relation:

            * We unpack a single object from the first row in the result set.
            * We check if the primary key of the nxt row (if it exists) is the same
            * If it is, we pop a row from the set and repeat
            * If not, we return the results 
              (the unpack_single_object will pop the object in that case)
            """
            objs = []
            while True:
                try:
                    objs.append(unpack_single_object(objects,params,nested = True))
                except TypeError:
                    #this is an empty object
                    break
                if len(objects) > 1 and objects[1][pk_key] == pk_value:
                    objects.pop(0)
                else:
                    break
            return objs

        def unpack_single_object(objects,params,nested = False):
            obj = objects[0]
            d = {'__lazy__' : params['lazy']}
            print d
            pk_column = params['table_fields']['pk']
            if obj[pk_column] is None:
                raise TypeError
            pk_value = obj[params['table_fields']['pk']]

            for key,field in params['table_fields'].items():
                d[key] = obj[field]

            for name,join_params in params['joins'].items():
                if 'relationship_table' in join_params['relation']:
                    #this is a many-to-many join
                    d[name] = unpack_many_to_many(objects,join_params,pk_column,pk_value)
                else:
                    try:
                        d[name] = unpack_single_object(objects,join_params,nested = True)
                    except TypeError:
                        d[name] = None
            if not nested:
                objects.pop(0)
            return d

        if self.include:
            include = copy.deepcopy(self.include)

            if not isinstance(include,(list,tuple)):
                raise AttributeError("include must be a list/tuple")
        else:
            include = ()

        if self.only:
            if isinstance(self.only,dict):
                only = set(self.only.keys())
            else:
                only = set(self.only)
            include = set(include)
            for only_key in only:
                include.add(only_key)

        self.include_joins = self.backend.get_include_joins(self.cls,include)

        process_fields_and_subkeys(self.include_joins['collection'],s_cte,self.include_joins,[])

        if joins:
            for i,j in enumerate(joins):
                s_cte = s_cte.outerjoin(*j)

        with self.backend.transaction(use_auto = False):
            try:
                result = self.backend.connection.execute(select(rows).select_from(s_cte))
                if result.returns_rows:
                    objects = list(result.fetchall())
                else:
                    objects = []
            except sqlalchemy.exc.ResourceClosedError:
                objects = None
                raise

        #we "fold" the objects back into one list structure
        self.objects = []

        pprint.pprint(self.include_joins)

        while objects:
            self.objects.append(unpack_single_object(objects,self.include_joins))

        pprint.pprint(self.objects)
        
        self.pop_objects = self.objects[:]

    def as_list(self):
        if self.objects is None:
            self.get_objects()
        return [self.deserialize(obj) for obj in self.objects]

    def __getitem__(self,key):
        if isinstance(key, slice):
            start, stop, step = key.start, key.stop, key.step
            if step != None:
                raise IndexError("SQL backend dos not support steps in slices")
            if key.start == None:
                start = 0
            if key.stop == None:
                stop = len(self)
            if start < 0:
                start = len(self) + start
            if stop < 0:
                stop = len(self) + stop
            qs = copy.copy(self)
            if start:
                qs.offset(start)
            qs.limit(stop-start)
            qs.objects = None
            qs.count = None
            return qs
        if self.objects is None:
            self.get_objects()
        return self.deserialize(self.objects[key])

    def pop(self,i = 0):
        if self.objects is None:
            self.get_objects()
        if self.pop_objects:
            return self.deserialize(self.pop_objects.pop())
        raise IndexError("pop from empty list")

    def filter(self,*args,**kwargs):
        qs = self.backend.filter(self.cls,*args,**kwargs)
        return self.intersect(qs)

    def intersect(self,qs):
        new_qs = QuerySet(self.backend,self.table,self.cls,select = intersect(self.get_select(),qs.get_select()))
        return new_qs

    def delete(self):
        with self.backend.transaction(use_auto = False):
            delete_stmt = self.table.delete().where(self.table.c.pk.in_(self.get_select(fields = [self.table.c.pk])))
            self.backend.connection.execute(delete_stmt)

    def get_fields(self):
        return [self.table]

    def get_select(self,fields = None):
        if self.select is not None:
            return self.select
        if fields is None:
            fields = self.get_fields()
        if self.extra_fields:
            fields.extend(self.extra_fields)
        s = select(fields)
        if self.joins:
            full_join = None
            for j in self.joins:
                if full_join is not None:
                    full_join = full_join.join(*j)
                else:
                    full_join = outerjoin(self.table,*j)
            s = s.select_from(full_join)

        if self.condition is not None:
            s = s.where(self.condition)
        if self.group_bys:
            s = s.group_by(*self.group_bys)
        if self.havings:
            for having in self.havings:
                s = s.having(having)
        if self.order_bys:
            s = s.order_by(*self.order_bys)
        if self._offset:
            s = s.offset(self._offset)
        if self._limit:
            s = s.limit(self._limit)
        return s

    def __len__(self):
        if self.count is None:
            with self.backend.transaction(use_auto = False):
                s = select([func.count()]).select_from(self.get_select(fields = [self.table.c.pk]).alias('count_select'))
                result = self.backend.connection.execute(s)
                self.count = result.first()[0]
                result.close()
        return self.count

    def distinct_pks(self):
        with self.backend.transaction(use_auto = False):
            s = self.get_select([self.table.c.pk]).distinct(self.table.c.pk)
            result = self.backend.connection.execute(s)
            return set([r[0] for r in result.fetchall()])
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __eq__(self, other):
        if isinstance(other, QuerySet): 
            if self.cls == other.cls and len(self) == len(other) \
              and self.distinct_pks() == other.distinct_pks():
                return True
        elif isinstance(other, list):
            if len(other) != len(self.keys):
                return False
            objs = list(self)
            if other == objs:
                return True
        return False

