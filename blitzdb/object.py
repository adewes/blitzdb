
class Object(object):

    """
    """

    def __init__(self,attributes = {},lazy = False):
        self.__dict__['_attributes'] = attributes
        self.__dict__['embed'] = False

        self._lazy = lazy

        if not 'pk' in attributes:
            self.pk = None

        if not self._lazy:
            self.initialize()

    def initialize(self):
        pass

    def _lazily_load(self):

        self._lazy = False

        if not 'pk' in self._attributes or not self._attributes['pk']:
            raise AttributeError("No primary key given!")
        if not hasattr(self,'_lazy_backend'):
            raise AttributeError("No backend for lazy loading given!")

        obj = self._lazy_backend.get(self.__class__,{'pk':self._attributes['pk']})
        del self._lazy_backend

        self._attributes = obj.attributes
        self.initialize()

    def __getattr__(self,key):

        if key.startswith('_'):
            return super(Object,self).__getattr__(key)
        else:
            if self._lazy:
                self._lazily_load()
            return self._attributes[key]

    def __setattr__(self,key,value):

        if key.startswith('_'):
            return super(Object,self).__setattr__(key,value)
        else:
            if self._lazy:
                self._lazily_load()
            self._attributes[key] = value

    def __delattr__(self,key):

        if key.startswith('_'):
            return super(Object,self).__delattr__(key)
        else:
            if self._lazy:
                self._lazily_load()

            if key in self._attributes:
                del self._attributes[key]

    @property
    def attributes(self):

        if self._lazy:
            self._lazily_load()

        return self._attributes

    def save(self,backend):

        if self._lazy:
            self._lazily_load()

        return backend.save(self)

    def delete(self,backend):

        if self._lazy:
            self._lazily_load()

        backend.delete(self)
        self.pk = None

    def __copy__(self):
        d = self.__class__(**self.attributes.copy())
        return d

    def __deepcopy__(self,memo):
        d = self.__class__(**copy.deepcopy(self.attributes,memo))
        return d

    def __ne__(self,other):
        return not self.__eq__(other)
    
    def __eq__(self,other):
        if id(self) == id(other):
            return True
        if type(self) != type(other):
            return False
        if self.pk == other.pk:
            return True
        if self.attributes == other.attributes:
            return True
        return False

    def _represent(self,n = 3):

        if n < 0:
            return self.__class__.__name__+"({...})"

        def truncate_dict(d,n = n):

            if isinstance(d,dict):
                out = {}
                return dict([(key,truncate_dict(value,n-1)) for key,value in d.items()])
            elif isinstance(d,list) or isinstance(d,set):
                return [truncate_dict(v,n-1) for v in d]
            elif isinstance(d,Object):
                return d._represent(n-1)
            else:
                return d

        if self._lazy:
            return self.__class__.__name__+"("+str(truncate_dict(self._attributes))+", lazy = True)"
        else:
            return self.__class__.__name__+"("+str(truncate_dict(self._attributes))+")"

    __str__ = __repr__ = _represent
