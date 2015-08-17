import time
import copy
import sqlalchemy

from blitzdb.queryset import QuerySet as BaseQuerySet
from functools import wraps
from sqlalchemy.sql import select,func,expression,delete
from sqlalchemy.sql.expression import join,asc,desc

class ASCENDING:
    pass

class DESCENDING:
    pass

class QuerySet(BaseQuerySet):

    def __init__(self, backend, table, connection,cls,
                 condition = None,select = None,intersects = None,raw = False,joins = None
                 ):
        super(QuerySet,self).__init__(backend = backend,cls = cls)

        self.joins = joins
        self.backend = backend
        self.condition = condition
        self.select = select
        self.cls = cls
        self.connection = connection
        self.table = table
        self._raw = raw
        self.count = None
        self.order_bys = None
        self.result = None
        self.intersects = intersects
        self.objects = None
        self.pop_objects = None

    def deserialize(self, data):
        if self._raw:
            return dict(data)
        deserialized_attributes = self.backend.deserialize(data)
        return self.backend.create_instance(self.cls, deserialized_attributes)

    def sort(self, columns):
        order_bys = []
        for column,direction in columns:
            if direction > 0:
                direction = asc
            else:
                direction = desc
            order_bys.append(direction(column))
        self.order_bys = order_bys
        self.objects = None
        return self

    def __iter__(self):
        if self.objects is None:
            self.get_objects()
        for obj in self.objects:
            yield self.deserialize(obj)
        raise StopIteration

    def get_objects(self):
        s = self.get_select()
        try:
            self.objects = self.connection.execute(s).fetchall()
        except sqlalchemy.exc.ResourceClosedError:
            self.objects = []
        self.pop_objects = self.objects[:]

    def as_list(self):
        if self.objects is None:
            self.get_objects()
        return [self.deserialize(obj) for obj in self.objects]

    def __getitem__(self,i):
        if self.objects is None:
            self.get_objects()
        return self.deserialize(self.objects[i])

    def pop(self,i = 0):
        if self.objects is None:
            self.get_objects()
        if self.pop_objects:
            return self.deserialize(self.pop_objects.pop())
        raise IndexError("No more results!")

    def filter(self,*args,**kwargs):
        qs = self.backend.filter(*args,**kwargs)
        return self.intersect(qs)

    def delete(self):
        if self.condition:
            delete_stmt = self.table.delete().where(self.condition)
        elif self.select:
            delete_stmt = self.table.delete().where(self.table.c.pk.in_(self.select.select_from([self.table.c.pk])))
        else:
            delete_stmt = self.table.delete()
        self.connection.execute(delete_stmt)

    def get_select(self,fields = None):
        if fields is None:
            fields = [self.table]
        if self.condition is not None:
            s = select(fields).where(self.condition)
        else:
            s = select(fields)
        if self.joins:
            for j in self.joins:
                s = s.select_from(join(self.table,*j))
        if self.order_bys:
            s = s.order_by(*self.order_bys)
        return s

    def intersect(self,queryset):
        s1 = self.get_select()
        s2 = queryset.get_select()
        if self.intersects:
            intersects = self.intersects[:]
            intersects.append(s2)
        else:
            intersects = [s1,s2]

        i = expression.intersect(*intersects)

        qs = QuerySet(backend = self.backend,table = self.table,
                      connection = self.connection,
                      select = i,intersects = intersects)
        return qs

    def __len__(self):
        if self.count is None:
            s = self.get_select(fields = [func.count(self.table.c.pk)])
            result = self.connection.execute(s)
            self.count = result.first()[0]
            result.close()
            print "Done",self.count
        return self.count
        
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __eq__(self, other):
        if isinstance(other, QuerySet): 
            if self.cls == other.cls and set(self._cursor.distinct('_id')) == set(other._cursor.distinct('_id')):
                return True
        elif isinstance(other, list):
            if len(other) != len(self.keys):
                return False
            objs = list(self)
            if other == objs:
                return True
        return False

