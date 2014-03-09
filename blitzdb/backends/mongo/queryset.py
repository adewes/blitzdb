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

    def _create_object_for(self,json_attributes):
        deserialized_attributes = self.backend.deserialize(json_attributes)
        if '_id' in deserialized_attributes:
            del deserialized_attributes['_id']
        return self.backend.create_instance(self.cls,deserialized_attributes)

    def next(self):
        json_attributes = self._cursor.next()
        obj = self._create_object_for(json_attributes)
        return obj

    def __getitem__(self,key):
        if isinstance(key,slice):
            return self.__class__(self.backend,self.cls,self._cursor.__getitem__(key))
        json_attributes = self._cursor[key]
        obj = self._create_object_for(json_attributes)
        return obj

    def __contains__(self,obj):
        pks = self._cursor.distinct('_id')
        if isinstance(obj,list) or isinstance(obj,tuple):
            obj_list = obj
        else:
            obj_list = [obj]
        for obj in obj_list:
            if not obj.pk in pks:
                return False
        return True

    def rewind(self):
        self._cursor.rewind()

    def delete(self):
        self._cursor.collection.remove({'_id' : {'$in' : self._cursor.distinct('_id') }})

    def filter(self,*args,**kwargs):
        return self.backend.filter(self.cls,*args,initial_keys = self.keys,**kwargs)

    def __len__(self):
        return self._cursor.count()

    def __ne__(self,other):
        return not self.__eq__(other)
    
    def __eq__(self,other):
        if isinstance(other,QuerySet): 
            if self.cls == other.cls and set(self._cursor.distinct('_id'))  == set(other._cursor.distinct('_id')):
                return True
        elif isinstance(other,list):
            if len(other) != len(self.keys):
                return False
            objs = list(self)
            if other == objs:
                return True
        return False
