from sqlalchemy.sql import select,func,expression,delete
from sqlalchemy.sql.expression import join,asc,desc,text,and_
from .queryset import QuerySet

class ManyToManyProxy(object):

    """
    The ManyToManyProxy transparently handles n:m relationships among different object types.
    It automatically retrieves related documents from the database and initializes them.

    From the outside, the behavior corresponds to that of a normal Python list to which we
    can append

    Open questions:

    * What happens if we copy a ManyToManyProxy to another object?
      Answer:
        The objects should be updated accordingly when the object gets saved to the database.

    :param        obj:
    :param field_name:
    :param     params:

    example::

        foo = bar

    """

    def __init__(self,obj,field_name,params,objects = None):
        """
        - Get the related class
        - Create a query that will retrieve related objects according to our criteria
          (either all elements of filtered by some key)
        - When requesting objects, use a QuerySet to retrieve it from the database.
        - When inserting/deleting objects, perform an INSERT against the database and
          invalidate the QuerySet object that we use to retrieve objects.
        """
        self.obj = obj
        self.collection = self.obj.backend.get_collection_for_obj(self.obj)
        self.field_name = field_name
        self.params = params
        self._objects = objects
        self._queryset = None

    def __call__(self,*args,**kwargs):
        self.get_queryset(*args,**kwargs)
        return self

    def __getitem__(self,i):
        if not isinstance(i,(slice,int)):
            raise TypeError("Index must be an integer or slice object")
        queryset = self.get_queryset()
        return queryset[i]

    def __setitem__(self,i,value):
        #there is (IMHO) no reasonable and non-ambiguous way to implement this in SQL...
        raise NotImplementedError

    def __contains__(self,item):
        queryset = self.get_queryset()
        return item in queryset

    def __delitem__(self,i):
        obj = self[i]
        self.remove(obj)

    def get_queryset(self,*args,**kwargs):
        if self._queryset is None:
            relationship_table = self.params['relationship_table']
            foreign_table = self.obj.backend.get_collection_table(self.params['collection'])
            condition = relationship_table.c['pk_%s' % self.collection] \
                == expression.cast(self.obj.pk,self.params['type'])
            self._queryset = QuerySet(backend = self.obj.backend,
                                      table = foreign_table,
                                      cls = self.params['class'],
                                      joins = [(relationship_table,)],
                                      condition = condition,
                                      objects = self._objects,
                                      *args,
                                      **kwargs)
        return self._queryset

    def append(self,obj):
        with self.obj.backend.transaction(implicit = True):
            relationship_table = self.params['relationship_table']
            condition = and_(relationship_table.c['pk_%s' % self.params['collection']] == obj.pk,
                             relationship_table.c['pk_%s' % self.collection] == self.obj.pk)
            s = select([func.count(text('*'))]).where(condition)
            result = self.obj.backend.connection.execute(s)
            cnt = result.first()[0]
            if cnt:
                return #the object is already inside
            values = {
                'pk_%s' % self.collection : self.obj.pk,
                'pk_%s' % self.params['collection'] : obj.pk
            }
            insert = relationship_table.insert().values(**values)
            self.obj.backend.connection.execute(insert)
            self._queryset = None

    def extend(self,objects):
        for obj in objects:
            self.append(obj)

    def insert(self,i,obj):
        raise NotImplementedError

    def delete(self):
        with self.obj.backend.transaction(implicit = True):
            condition = relationship_table.c['pk_%s' % self.collection] == self.obj.pk
            self.obj.bckend.connection.execute(delete(relationship_table).where(condition))

    def remove(self,obj):
        """
        Remove an object from the relation
        """
        with self.obj.backend.transaction(implicit = True):
            relationship_table = self.params['relationship_table']
            condition = and_(relationship_table.c['pk_%s' % self.params['collection']] == obj.pk,
                             relationship_table.c['pk_%s' % self.collection] == self.obj.pk)
            self.obj.backend.connection.execute(delete(relationship_table).where(condition))
            self._queryset = None

    def pop(self,i = None):
        queryset = self.get_queryset()
        return queryset.pop(i)

    def __len__(self):
        queryset = self.get_queryset()
        return len(queryset)
