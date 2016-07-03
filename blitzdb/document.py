import copy
import uuid

from blitzdb.fields.base import BaseField
from blitzdb.fields import CharField

import logging

logger = logging.getLogger(__name__)

import six

if six.PY3:
    unicode = str

class DoesNotExist(BaseException):

    def __str__(self):
        return "DoesNotExist(%s)" % self.__class__.__name__

class MultipleDocumentsReturned(BaseException):

    def __str__(self):
        return "MultipleDocumentsReturned(%s)" % self.__class__.__name__

class MetaDocument(type):

    """
    Here we inject class-dependent exceptions into the Document class.
    """

    def __new__(meta, name, bases, dct):
        sanitized_dct = {}
        sanitized_dct['fields'] = {}

        class_type = type.__new__(meta, name, bases, dct)

        fields = {}

        #we inherit the fields from the base type(s)
        if hasattr(class_type,'fields'):
            fields.update(class_type.fields)

        for key,value in dct.items():
            if isinstance(value,BaseField):
                if value.key:
                    field_key = value.key
                else:
                    field_key = key
                fields[field_key] = value
                delattr(class_type,key)

        class_type.fields = fields

        global DoesNotExist,MultipleDocumentsReturned

        class DoesNotExist(DoesNotExist):
            pass

        class MultipleDocumentsReturned(MultipleDocumentsReturned):
            pass

        class_type.DoesNotExist = DoesNotExist
        class_type.MultipleDocumentsReturned = MultipleDocumentsReturned

        if class_type in document_classes:
            document_classes.remove(class_type)
        if name == 'Document' and bases == (object,):
            pass
        elif not (hasattr(class_type.Meta,'autoregister') and class_type.Meta.autoregister == False):
            document_classes.append(class_type)

        return class_type

document_classes = []

@six.add_metaclass(MetaDocument)
class Document(object):

    """
    The Document object is the base class for all documents stored in the database.
    The name of the collection can be set by defining a :class:`Document.Meta` class within
    the class and setting its `collection` attribute.

    :param attributes: the attributes of the document instance. Expects a Python dictionary.
    :param lazy: if set to `True`, will lazily load the document from the backend when
                 an attribute is requested. This requires that `backend` has been
                 specified and that the `pk` attribute is set. 

    :param backend: the backend to be used for saving and loading the document.


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

       print("%s was born in %d" % (marlon.name,marlon.birth_year))

    In case one of your attributes shadows a class attribute or function, you can still access it
    using the `attributes` attribute.

    example::

      fail = Document({'delete': False,'save' : True})

      print(fail.delete) #will print <bound method Document.save ...>

      print(fail.attributes['delete']) #will print 'False'

    **Defining "non-database" attributes**

    Attributes that begin with an underscore (_) will not be stored in the :py:meth:`attributes`
    dictionary but as normal instance attributes of the document. This is useful if you need to
    define e.g. some helper variables that you don't want to store in the database.
    """

    abstract = True

    class Meta:

        PkType = CharField(length = 32,primary_key = True,indexed = True,nullable = False)
        primary_key = "pk"
        indexes = {}

    def __init__(self, attributes=None, lazy=False, backend=None, autoload=True, db_loader = None):
        """
        Initializes a document instance with the given attributes. If `lazy = True`, a *lazy* 
        document will be created, which means that the attributes of the document will be loaded 
        from the database only if they are requested. Lazy loading requires that the `backend` 
        variable is set.

        :param attributes: the attributes of the document instance.

        :param lazy: specifies if the document is *lazy*, i.e. if it should be loaded on demand 
                     when its attributes get accessed for the first time.

        :param backend: the backend for use in the `save`, `delete` and `revert` functions.

        """
        if not attributes:
            attributes = {}
        self._attributes = attributes
        self._autoload = autoload
        self._backend = backend
        self._properties = {}
        self._db_loader = db_loader

        if not lazy:
            self._lazy = False
        else:
            self._lazy = True

        self._embed = False
        self.initialize()

    def __getitem__(self,key):
        try:
            lazy = super(Document,self).__getattribute__('_lazy')
        except AttributeError:
            lazy = False
        if lazy:
            if key in self.lazy_attributes:
                return self.lazy_attributes[key]
            elif self._autoload:
                self.revert()
        return self.attributes[key]

    @property
    def lazy(self):
        return self._lazy

    @lazy.setter
    def lazy(self,lazy):
        self._lazy = lazy

    @property
    def lazy_attributes(self):
        return self._attributes

    @property
    def attributes(self):
        if self._lazy and self._autoload:
            self.revert()
        return self._attributes

    @attributes.setter
    def attributes(self,value):
        self._attributes = value

    def get(self,key,default = None):
        return self[key] if key in self else default

    def has_key(self,key):
        return True if key in self else False

    def keys(self):
        return self.attributes.keys()

    def clear(self):
        self.attributes.clear()

    def values(self):
        return self.attributes.values()

    def items(self):
        return self.attributes.items()

    @property
    def properties(self):
        return self._properties

    @properties.setter
    def properties(self,value):
        self._properties = value

    def __contains__(self, key):
        return True if (key in self.lazy_attributes or key in self.attributes) else False

    def __iter__(self):
        for key in self.keys():
            yield key

    def get_lazy_attribute(self,key):
        #we make sure not to revert the document...
        return object.__getattribute__(self,key)

    def __getattr__(self, key,load_if_lazy = True):

        try:
            return super(Document,self).__getattr__(key)
        except AttributeError:
            pass
        try:
            if self._lazy and self._autoload:
                self.revert()
            if key in self._properties:
                return self._properties[key]
            return self._attributes[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        if key.startswith('_') or key in ('attributes','pk','lazy','backend'):
            return super(Document, self).__setattr__(key, value)
        else:
            self.attributes[key] = value

    def __delattr__(self, key):
        if key.startswith('_'):
            return super(Document, self).__delattr__(key)
        try:
            del self.attributes[key]
        except KeyError:
            raise AttributeError(key)

    __setitem__ = __setattr__

    def __delitem__(self, key):
        try:
            return self.__delattr__(key)
        except AttributeError:
            raise KeyError(key)

    def __copy__(self):
        d = self.__class__(self.attributes.copy(), lazy=self._lazy, backend = self._backend)
        return d

    def __deepcopy__(self, memo):
        d = self.__class__(copy.deepcopy(self.attributes, memo), 
                           lazy=self._lazy,
                           backend =self._backend)
        return d

    def __hash__(self):
        return id(self)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __nonzero__(self):
        if self.pk:
            return True
        return False

    def __eq__(self, other):
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

    def __unicode__(self):
        return self.__class__.__name__ + "({{{0} : '{1}'}},lazy = {2})".format(self.get_pk_name(), self.pk, self._lazy)

    if six.PY3:
        __str__ = __unicode__
    else:
        def __str__(self):
            return unicode(self).encode("utf-8")

    def _represent(self, n=1):

        if n < 0:
            return self.__class__.__name__ + "({...})"

        def truncate_dict(d, n=n):

            if isinstance(d, dict):
                out = {}
                return dict([(key, truncate_dict(value, n - 1)) for key, value in d.items()])
            elif isinstance(d, list) or isinstance(d, set):
                return [truncate_dict(v, n - 1) for v in d]
            elif isinstance(d, Document):
                return d._represent(n - 1)
            else:
                return d

        return self.__class__.__name__ + "(" + str(truncate_dict(self._attributes)) + ")"

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
        if you save a document without a primary key field. By default, it uses `uuid.uuid4().hex`
        to generate a (statistically) unique primary key for the object (`more about UUIDs 
        <http://docs.python.org/2/library/uuid.html/>`_). 
        If you want to define your own primary key generation mechanism, just redefine this function
        in your document class.
        """
        self.pk = uuid.uuid4().hex 

    @classmethod
    def get_pk_name(cls):
        return cls.Meta.primary_key if hasattr(cls.Meta, 'primary_key') else Document.Meta.primary_key

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
            return self._attributes[primary_key]

        #if there is no pk value but a _db_loader, we load the object lazily to retrieve the pk
        if self._lazy and self._db_loader:
            self.revert()
            return self.pk

        return None

    @property
    def embed(self):
        return self._embed

    @property
    def eager(self):
        return self.load_if_lazy()

    @pk.setter
    def pk(self, value):
        self._attributes[self.get_pk_name()] = value

    @property
    def backend(self):
        return self._backend

    @backend.setter
    def backend(self,backend):
        self._backend = backend

    def save(self, backend=None):
        """
        Saves a document to the database. If the `backend` argument is not specified, 
        the function resorts to the *default backend* as defined during object instantiation. 
        If no such backend is defined, an `AttributeError` exception will be thrown.

        :param backend: the backend in which to store the document.

        """
        if not backend:
            if not self._backend:
                raise AttributeError("No default backend defined!")
            return self._backend.save(self)
        self._backend = backend
        return backend.save(self)

    def delete(self, backend=None):
        """
        Deletes a document from the database. If the `backend` argument is not specified,
        the function resorts to the *default backend* as defined during object instantiation.
        If no such backend is defined, an `AttributeError` exception will be thrown.

        :param backend: the backend from which to delete the document.

        """
        if not backend:
            if not self._backend:
                raise AttributeError("No default backend defined!")
            return self._backend.delete(self)
        backend.delete(self)

    def revert(self, backend=None):
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
        self._lazy = False
        logger.debug("Reverting to database state (%s, %s)" % (self.__class__.__name__, str(self.pk)))
        if self._db_loader:
            obj = self._db_loader()
        else:
            backend = backend or self._backend
            if not backend:
                raise AttributeError("No backend given!")
            if self.pk is None:
                return
            obj = backend.get(self.__class__, {self.get_pk_name(): self.pk})
        self._attributes = obj.attributes
        self.initialize()

    def load_if_lazy(self):
        if self._lazy:
            self.revert()
        return self
