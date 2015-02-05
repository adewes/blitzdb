from blitzdb.queryset import QuerySet as BaseQuerySet
from functools import wraps


class QuerySet(object):

    def with_result(f):

        def with_result_decorator(self,*args,**kwargs):
            if self.result is None:
                self.execute()
            return f(self,*args,**kwargs)

        return with_result_decorator

    def __init__(self, backend, table, connection, deserializer,
                 condition = None,select = None,extra_fields = None,
                 ):
        self.store = store
        self.condition = condition
        self.select = select
        self.deserializer = deserializer
        self.connection = connection
        self.table = table
        self.extra_fields = extra_fields if extra_fields is not None else []
        self.count = None
        self.result = None
        self.objects = []

    def execute(self):
        s = self.get_select()
        self.result = self.connection.execute(s)

    @with_result
    def __iter__(self):
        while True:
            result = self.result.fetchone()
            if result:
                yield self.deserializer(result)
            else:
                raise StopIteration

    @with_result
    def pop(self,i = 0):
        if i != 0:
            raise NotImplementedError
        result = self.result.fetchone()
        if result:
            if self.count is None:
                len(self)
            self.count = self.count - 1
            return self.deserializer(result)
        raise IndexError("No more results!")

    def get_select(self):
        if self.condition is not None:
            s = select([self.table]).where(self.condition)
        elif self.select is not None:
            s = self.select
        else:
            s = select([self.table])
        return s

    def intersect(self,queryset):
        s1 = self.get_select()
        s2 = queryset.get_select()
        i = expression.intersect(s1,s2)
        qs = QuerySet(self.store,self.table,self.connection,self.deserializer,select = i)
        return qs

    def __len__(self):
        if self.count is None:
            s = self.get_select()
            count_select = select([func.count(s.alias("count").c.pk)])
            self.count = self.connection.execute(count_select)\
                             .first()[0]
        return self.count
        

