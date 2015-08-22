from blitzdb.fields import BaseField

class ForeignKeyField(BaseField):

    """
    The ManyToManyProxy should support the following operations:

    - Retrieve related documents from the database
    - Append new documents to the relation
    - Remove documents from the relation
    """

    def __init__(self,related,**kwargs):
        super(ForeignKeyField,self).__init__(**kwargs)
        self.related = related

    def __getitem__(self,item):
        raise NotImplementedError

    def __setitem__(self,item,value):
        raise NotImplementedError

    def __delitem__(self,item):
        raise NotImplementedError

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
