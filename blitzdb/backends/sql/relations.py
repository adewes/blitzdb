

class ManyToManyField(object):

    """
    The ManyToManyProxy should support the following operations:

    - Retrieve related documents from the database
    - Append new documents to the relation
    - Remove documents from the relation
    """

    def __init__(self,backend,obj,field_name,params):
        self.backend = backend
        self.obj = obj
        self.field_name = field_name
        self.params = params

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

class ListField(object):

    """
    Manages a related list of (non-document) objects of uniform type, such as tags.

    .. admonition:: Registering classes
        
        The SQL backend ensures that there are no duplicate elements in the related list.

        Operations sensitive 

    """

    def __init__(self,backend,obj,field_name,params):
        self.backend = backend
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
