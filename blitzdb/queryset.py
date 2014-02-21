class QuerySet(object):

    """
    A queryset accepts a list of store keys and returns the corresponding objects on demand.

    To Do:

    -Implement slice operators
    -Improve deletion efficiency (do not fetch objects before deleting them)
    """

    def __init__(self,backend,store,cls,keys):
        self.cls = cls
        self.store = store
        self.keys = list(keys)
        self.backend = backend
        self.objects = {}

    def __getitem__(self,i):
        key = self.keys[i]
        if not key in self.objects:
            self.objects[key] = self.backend.get_object(self.cls,key)
            self.objects[key]._store_key = key
        return self.objects[key]

    def delete(self):
        for i in range(0,len(self.keys)):
            try:
                obj = self[i]
                self.backend.delete(obj)
            except AttributeError:
                pass
        self.keys = []
        self.objects = {}

    def filter(self,*args,**kwargs):
        return self.backend.filter(self.cls,*args,initial_keys = self.keys,**kwargs)

    def __len__(self):
        return len(self.keys)

    def __ne__(self,other):
        return not self.__eq__(other)
    
    def __eq__(self,other):
        if isinstance(other,QuerySet): 
            if self.cls == other.cls and set(self.keys) == set(other.keys):
                return True
        elif isinstance(other,list):
            if len(other) != len(self.keys):
                return False
            objs = list(self)
            if other == objs:
                return True
        return False

    def __req__(self,other):
        raise Exception