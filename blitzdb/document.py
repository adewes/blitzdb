import copy
import uuid

class MetaDocument(type):

    """
    Here we inject class-dependent exceptions into the Document class.
    """

    def __new__(meta,name,bases,dct):

        class DoesNotExist(BaseException):

            def __str__(self):
                return "DoesNotExist(%s)" % name

        class MultipleDocumentsReturned(BaseException):

            def __str__(self):
                return "MultipleDocumentsReturned(%s)" % name

        dct['DoesNotExist'] = DoesNotExist
        dct['MultipleDocumentsReturned'] = MultipleDocumentsReturned

        class_type = type.__new__(meta, name, bases, dct)

        if class_type in document_classes:
            document_classes.remove(class_type)
        if name == 'Document' and bases == (BaseDocument,):
            pass
        else:
            document_classes.append(class_type)
        return class_type

document_classes = []


class BaseDocument(object):

    """
    The Document object is the base class for all documents stored in the database.
    The name of the collection can be set by defining a :class:`Document.Meta` class within
    the class and setting its `collection` attribute.

    :param attributes: the attributes of the document instance. Expects a Python dictionary.
    :param lazy: if set to `True`, will lazily load the document from the backend when
                 an attribute is requested. This requires that `default_backend` has been
                 specified and that the `pk` attribute is set. 

    :param default_backend: the default backend to be used for saving and loading the document.


    **The `Meta` attribute**

    You can use the `Meta` attribute of the class to specify the primary_key (defaults to `pk`)
    or change the collection name for a given class.

    example::

        class Actor(Document):

            class Meta(Document.Meta):
                pk = 'name'
                collection = 'hollywood_actors'

    **Accessing Document Attributes**

    Document attributes are accessible as class attributes:

    .. code-block:: python

       marlon = Actor({'name' : 'Marlon Brando', 'birth_year' : 1924})

       print "%s was born in %d" % (marlon.name,marlon.birth_year)

    In case one of your attributes shadows a class attribute or function, you can still access it
    using the `attributes` attribute.

    example::

      fail = Document({'delete': False,'save' : True})

      print fail.delete #will print <bound method Document.save ...>

      print fail.attributes['delete'] #will print 'False'

    **Defining "non-database" attributes**

    Attributes that begin with an underscore (_) will not be stored in the :py:meth:`attributes`
    dictionary but as normal instance attributes of the document. This is useful if you need to 
    define e.g. some helper variables that you don't want to store in the database.
    """

    class Meta:

        primary_key = "pk"

    def __init__(self,attributes = None,lazy = False,default_backend = None):
        """
        Initializes a document instance with the given attributes. If `lazy = True`, a *lazy* document
        will be created, which means that the attributes of the document will be loaded from the database
        only if they are requested. Lazy loading requires that the `default_backend` variable is set.

        :param attributes: the attributes of the document instance.

        :param lazy: specifies if the document is *lazy*, i.e. if it should be loaded on demand when its attributes get accessed for the first time.

        :param default_backend: the default backend for use in the `save`, `delete` and `revert` functions.

        """
        if not attributes:
            attributes = {}
        self.__dict__['_attributes'] = attributes
        self.__dict__['embed'] = False
        self._default_backend = default_backend
        if self.pk is None:
            self.pk = None

        if not lazy:
            self._lazy = False
            self.initialize()
        else:
            self._lazy = True

    def __getattribute__(self,key):
        """
        Checks if the `_lazy` attribute of the document is set. If this is the case, the function
        lazily loads the document from the database by calling `revert` and sets `_lazy = False'
        after doing so.
        """
        try:
            lazy = super(BaseDocument,self).__getattribute__('_lazy')
        except AttributeError:
            lazy = False
        if lazy:
            if key == 'lazy_attributes':
                return super(BaseDocument,self).__getattribute__('_attributes')
            #If we demand the attributes, we load the object from the DB in any case.
            if key in ('attributes',):
                self.revert()
                self._lazy = False
            try:
                return super(BaseDocument,self).__getattribute__(key)
            except AttributeError:
                pass
            self._lazy = False
            self.revert()
        return super(BaseDocument,self).__getattribute__(key)

    def keys(self):
        return self.attributes.keys()

    def values(self):
        return self.attributes.values()

    def items(self):
        return self.attributes.items()

    def __contains__(self,key):
        return True if key in self.attributes else False

    def __getattr__(self,key):
        try:
            super(BaseDocument,self).__getattr__(key)
        except AttributeError:
            try:
                return self.attributes[key]
            except KeyError:
                raise AttributeError(key)

    def __setattr__(self,key,value):
        if key.startswith('_'):
            return super(BaseDocument,self).__setattr__(key,value)
        elif key == 'pk':
            #this is ugly, should find a better solution for handling properties...
            super(BaseDocument,self).__setattr__(key,value)
        else:
            self.attributes[key] = value

    def __delattr__(self,key):
        if key.startswith('_'):
            return super(BaseDocument,self).__delattr__(key)
        try:
            del self.attributes[key]
        except KeyError:
            raise AttributeError(key)

    def __getitem__(self,key):
        return self.attributes[key]

    __setitem__ = __setattr__

    def __delitem__(self,key):
        try:
            return self.__delattr__(key)
        except AttributeError:
            raise KeyError(key)

    def __copy__(self):
        d = self.__class__(self.attributes.copy(),lazy = self._lazy,default_backen = self._default_backend)
        return d

    def __deepcopy__(self,memo):
        d = self.__class__(copy.deepcopy(self.attributes,memo),lazy = self._lazy,default_backend = self._default_backend)
        return d

    def __hash__(self):
        return id(self)

    def __ne__(self,other):
        return not self.__eq__(other)
    
    def __eq__(self,other):
        """
        Compares the document instance to another object. The comparison rules are as follows:

        * If the Python `id` of the objects are identical, return `True`
        * If the types of the objects differ, return `False`
        * If the types match and the primary keys are identical, return `True`
        * If the types and attributes of the objects match, return `True`
        * Otherwise, return `False`
        """
        if id(self) == id(other):
            return True
        if type(self) != type(other):
            return False
        if self.pk != None or other.pk != None:
            if self.pk == other.pk:
                return True
        if self.attributes == other.attributes:
            return True
        return False

    def __str__(self):
        return unicode(self).encode("utf-8")

    def __unicode__(self):
        return self.__class__.__name__+"({'pk' : '%s'},lazy = %s)" % (str(self.pk),str(self._lazy))

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

    __repr__ = _represent

    def initialize(self):
        """
        Gets called when **after** the object attributes get loaded from the database. 
        Redefine it in your document class to perform object initialization tasks.

        .. admonition:: Keep in Mind

            The function also get called after invoking the `revert` function, which 
            resets the object attributes to those in the database, so do not assume that
            the function will get called only once during the lifetime of the object.

            Likewise, you should **not** perform any initialization in the `__init__` 
            function to initialize your object, since this can possibly break lazy loading 
            and `revert` operations.
        """
        pass

    def autogenerate_pk(self):
        """
        Autogenerates a primary key for this document. This function gets called by the backend
        if you save a document without a primary key field. By default, it uses `uuid.uuid1().hex`
        to generate a (statistically) unique primary key for the object (`more about UUIDs <http://docs.python.org/2/library/uuid.html/>`_). 
        If you want to define your own primary key generation mechanism, just redefine this function
        in your document class.
        """
        self.pk = uuid.uuid1().hex 

    @classmethod
    def get_pk_name(cls):
        return cls.Meta.primary_key if hasattr(cls.Meta,'primary_key') else Document.Meta.primary_key

    @property
    def pk(self):
        """
        Returns (or sets) the primary key of the document, which is stored in the `attributes` dict
        along with all other attributes. The name of the primary key defaults to `pk` and 
        can be redefine in the `Meta` class. This function provides a standardized way to
        retrieve and set the primary key of a document and is used by the backend and a 
        few other classes. If possible, always use this function to access the
        primary key of a document.

        .. admonition:: Automatic primary key generation

            If you save a document to the database that has an empty primary key field,
            Blitz will create a default primary-key by calling the `autogenerate_pk` function
            of the document. To generate your own primary keys, just redefine this function
            in your derived document class.

        """
        primary_key = self.get_pk_name()
        if primary_key in self._attributes:
            return self._attributes[self.Meta.primary_key]
        return None

    @pk.setter
    def pk(self, value):
        self._attributes[self.Meta.primary_key] = value    

    @property
    def attributes(self):
        """
        Returns a reference to the attributes of the document. The attributes are the *"unique source of truth"*
        about the state of a document.
        """
        return self._attributes

    def save(self,backend = None):
        """
        Saves a document to the database. If the `backend` argument is not specified, the function resorts
        to the *default backend* as defined during object instantiation. If no such backend is defined, an
        `AttributeError` exception will be thrown.

        :param backend: the backend in which to store the document.

        """
        if not backend:
            if not self._default_backend:
                raise AttributeError("No default backend defined!")
            return self._default_backend.save(self)
        return backend.save(self)

    def delete(self,backend = None):
        """
        Deletes a document from the database. If the `backend` argument is not specified, the function resorts
        to the *default backend* as defined during object instantiation. If no such backend is defined, an
        `AttributeError` exception will be thrown.

        :param backend: the backend from which to delete the document.

        """
        if not backend:
            if not self._default_backend:
                raise AttributeError("No default backend defined!")
            return self._default_backend.delete(self)
        backend.delete(self)

    def revert(self,backend = None):
        """
        Reverts the state of the document to that contained in the database. 
        If the `backend` argument is not specified, the function resorts to the *default backend* 
        as defined during object instantiation. If no such backend is defined, an `AttributeError` 
        exception will be thrown.

        :param backend: the backend from which to delete the document.

        .. admonition:: Keep in Mind

            This function will call the `initialize` function after loading the object, which
            allows you to perform document-specific initialization tasks if needed.

        """
        backend = backend or self._default_backend
        if not backend:
            raise AttributeError("No backend for lazy loading given!")
        if self.pk == None:
            raise self.DoesNotExist("No primary key given!")
        obj = self._default_backend.get(self.__class__,{'pk':self.pk})
        self._attributes = obj.attributes
        self.initialize()

Document = MetaDocument('Document', (BaseDocument,), {})
