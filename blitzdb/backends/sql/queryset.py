from blitzdb.queryset import QuerySet as BaseQuerySet
from functools import wraps

class QuerySet(BaseQuerySet):

    """
    """

    def __init__(self,backend,cls,cursor):
        super(QuerySet,self).__init__(backend,cls)
        self._cursor = cursor
        
    def __iter__(self):
        return self

    def __len__(self):
        pass

    def _create_object_for(self,json_attributes):
        pass

    def next(self):
        pass

    __next__ = next

    def __getitem__(self,key):
        pass

    def __contains__(self,obj):
        pass

    def rewind(self):
        pass

    def delete(self):
        pass

    def sort(self,*args,**kwargs):
        pass

    def filter(self,*args,**kwargs):
        pass

    def __len__(self):
        pass

    def __ne__(self,other):
        pass
    
    def __eq__(self,other):
        pass
