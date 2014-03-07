import copy

class MetaDocument(type):

    """
    Here we inject class-dependent exceptions into the Document class.
    """

    def __new__(meta,name,bases,dct):

        class DoesNotExist(BaseException):
            pass

        class MultipleDocumentsReturned(BaseException):
            pass
        dct['DoesNotExist'] = DoesNotExist
        dct['MultipleDocumentsReturned'] = MultipleDocumentsReturned
        class_type = type.__new__(meta, name, bases, dct)
        if not class_type in document_classes:
            document_classes.append(class_type)
        return class_type

document_classes = []

class Document(object):

    """
    The base class for all documents stored in the database.
    """

    __metaclass__ = MetaDocument

    class Meta:
        primary_key = "pk"

    def __init__(self,attributes = None,lazy = False,default_backend = None):
        if not attributes:
            attributes = {}
        self.__dict__['_attributes'] = attributes
        self.__dict__['embed'] = False
        self._default_backend = default_backend
        if not self.pk:
            self.pk = None

        if not lazy:
            self.initialize()
        else:
            self._lazy = True

    def revert(self,backend = None):
        backend = backend or self._default_backend
        if not backend:
            raise AttributeError("No backend for lazy loading given!")
        if self.pk == None:
            raise self.DoesNotExist("No primary key given!")
        obj = self._default_backend.get(self.__class__,{'pk':self.pk})
        self._attributes = obj.attributes
        self.initialize()

    def initialize(self):
        pass

    @property
    def pk(self):
        primary_key = self.Meta.primary_key if hasattr(self.Meta,'primary_key') else Document.Meta.primary_key
        if primary_key in self._attributes:
            return self._attributes[self.Meta.primary_key]
        return None

    @pk.setter
    def pk(self, value):
        self._attributes[self.Meta.primary_key] = value    

    def __getattribute__(self,key):
        try:
            lazy = super(Document,self).__getattribute__('_lazy')
        except AttributeError:
            lazy = False
        if lazy:
            self._lazy = False
            self.revert()
        return super(Document,self).__getattribute__(key)

    def __getattr__(self,key):
        try:
            super(Document,self).__getattr__(key)
        except AttributeError:
            return self._attributes[key]

    def __setattr__(self,key,value):
        if key.startswith('_'):
            return super(Document,self).__setattr__(key,value)
        else:
            self._attributes[key] = value

    def __delattr__(self,key):
        if key.startswith('_'):
            return super(Document,self).__delattr__(key)
        elif key in self._attributes:
                del self._attributes[key]

    @property
    def attributes(self):
         return self._attributes

    def save(self,backend = None):
        if not backend:
            if not self._default_backend:
                raise AttributeError("No default backend defined!")
            return self._default_backend.save(self)
        return backend.save(self)

    def delete(self,backend = None):
        if not backend:
            if not self._default_backend:
                raise AttributeError("No default backend defined!")
            return self._default_backend.delete(self)
        backend.delete(self)

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

    def _represent(self,n = 1):

        if n < 0:
            return self.__class__.__name__+"({...})"

        def truncate_dict(d,n = n):

            if isinstance(d,dict):
                out = {}
                return dict([(key,truncate_dict(value,n-1)) for key,value in d.items()])
            elif isinstance(d,list) or isinstance(d,set):
                return [truncate_dict(v,n-1) for v in d]
            elif isinstance(d,Document):
                return d._represent(n-1)
            else:
                return d

        return self.__class__.__name__+"("+str(truncate_dict(self._attributes))+")"

    __str__ = __repr__ = _represent
