from sqlalchemy.sql import select,func,expression,delete
from sqlalchemy.sql.expression import join,asc,desc
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

    def __init__(self,obj,field_name,params):
        """
        - Get the related class
        - Create a query that will retrieve related objects according to our criteria
          (either all elements of filtered by some key)
        - When requesting objects, use a QuerySet to retrieve it from the database.
        - When inserting/deleting objects, perform an INSERT against the database and
          invalidate the QuerySet object that we use to retrieve objects.
        """
        self.obj = obj
        self.field_name = field_name
        self.params = params
        self._queryset = None

    def __getitem__(self,item):
        queryset = self.get_queryset()
        return queryset[item]

    def __setitem__(self,item,value):
        raise NotImplementedError

    def __delitem__(self,item):
        raise NotImplementedError

    def get_queryset(self):
        if not self._queryset:
            relationship_table = self.params['relationship_table']
            foreign_table = self.obj.backend.get_collection_table(self.params['collection'])
            collection = self.obj.backend.get_collection_for_obj(self.obj)
            condition = relationship_table.c['pk_%s' % collection] \
                == expression.cast(self.obj.pk,self.obj.backend.Meta.PkType)
            self._queryset = QuerySet(backend = self.obj.backend,
                                      table = foreign_table,
                                      cls = self.params['class'],
                                      joins = [relationship_table],
                                      condition = condition)
        return self._queryset


    def append(self,obj):
        raise NotImplementedError

    def extend(self,objects):
        raise NotImplementedError

    def insert(self,i,obj):
        raise NotImplementedError

    def remove(self,obj):
        """
        Remove an object from the relation
        """
        raise NotImplementedError

    def pop(self,i = None):
        raise NotImplementedError

    def index(self,obj):
        raise NotImplementedError

    def count(self,obj):
        raise NotImplementedError

    def reverse(self,obj):
        raise NotImplementedError

class ListProxy(object):

    """
    Manages a related list of (non-document) objects of uniform type, such as tags.

    .. admonition:: Registering classes
        
        The SQL backend ensures that there are no duplicate elements in the related list.

        Operations sensitive 

    """

    def __init__(self,obj,field_name,params):
        self.obj = obj
        self.field_name = field_name
        self.params = params

    def sort(self,direction):
        pass

    def __getitem__(self,item):
        raise NotImplementedError

    def __setitem__(self,item,value):
        """

        .. admonition::

            Using __setitem__ makes only sense if the elements of the related list are ordered.
            Hence, when calling __setitem__, __getitem__ or __delitem__ without
        """
        raise NotImplementedError

    def __delitem__(self,item):
        raise NotImplementedError

    def append(self,obj):
        raise NotImplementedError

    def extend(self,objs):
        """
        Extend the list with the given objects.
        """
        raise NotImplementedError

    def remove(self,obj):
        """
        Remove an object from the relation
        """
        raise NotImplementedError

    def pop(self):
        """
        Pop an object from the related list.

        .. admonition:: 

            Related lists of objects will be retrieved unordered by default. The result of the pop
            operation might thus depend on the state of the database system and might not be
            reproducible for identical lists.
        """
        raise NotImplementedError
