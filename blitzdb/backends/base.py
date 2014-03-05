import abc
import inspect

from blitzdb.document import Document,document_classes

class NotInTransaction(BaseException):
    pass

class DatabaseIndexError(BaseException):
    pass
    
class Backend(object):

    class Meta(object):
        supports_indexes = False
        supports_transactions = False

    __metaclass__ = abc.ABCMeta

    def __init__(self,autodiscover_classes = True):
        self.classes = {}
        self.collections = {}
        self.primary_key_name = 'pk'
        if autodiscover_classes:
            self.autodiscover_classes()

    def autodiscover_classes(self):
        for document_class in document_classes:
            self.register(document_class)

    def register(self,cls,parameters = None):
        if not parameters:
            parameters = {}
        self.classes[cls] = parameters
        if 'collection' in parameters:
            self.collections[parameters['collection']] = cls
        else:
            self.collections[cls.__name__.lower()] = cls
            self.classes[cls]['collection'] = cls.__name__.lower()

    def autoregister(self,cls):
        def get_user_attributes(cls):
            boring = dir(type('dummy', (object,), {}))
            return dict([item
                    for item in inspect.getmembers(cls)
                    if item[0] not in boring])
        
        if hasattr(cls,'Meta'):
            params = get_user_attributes(cls.Meta)
        else:
            params = {}
        return self.register(cls,params)

    def serialize(self,obj,convert_keys_to_str = False,embed_level = 0):        

        serialize_with_opts = lambda value,*args,**kwargs : self.serialize(value,*args,convert_keys_to_str = convert_keys_to_str,**kwargs)

        if isinstance(obj,dict):
            output_obj = {}
            for key,value in obj.items():
                output_obj[str(key) if convert_keys_to_str else key] = serialize_with_opts(value,embed_level = embed_level)
        elif isinstance(obj,list):
            output_obj = map(lambda x:serialize_with_opts(x,embed_level = embed_level),obj)
        elif isinstance(obj,tuple):
            output_obj = tuple(map(lambda x:serialize_with_opts(x,embed_level = embed_level),obj))
        elif isinstance(obj,Document):
            collection = self.get_collection_for_obj(obj)
            if embed_level > 0:
                output_obj = serialize_with_opts(obj.attributes,embed_level = embed_level-1)
            elif obj.embed:
                output_obj = {'_collection':collection,'_attributes':serialize_with_opts(obj.attributes)}
            else:
                if obj.pk == None:
                    obj.save(self)
                output_obj = {self.primary_key_name:obj.pk,'_collection':self.classes[obj.__class__]['collection']}
        else:
            output_obj = obj
        return output_obj


    def deserialize(self,obj):
        if isinstance(obj,dict):
            if '_collection' in obj and self.primary_key_name in obj and obj['_collection'] in self.collections:
                output_obj = self.create_instance(obj['_collection'],{self.primary_key_name : obj[self.primary_key_name]},lazy = True)
            else:
                output_obj = {}
                for (key,value) in obj.items():
                    output_obj[key] = self.deserialize(value)
        elif isinstance(obj,list) or isinstance(obj,tuple):
            output_obj = map(lambda x:self.deserialize(x),obj)
        else:
            output_obj = obj
        return output_obj

    def create_instance(self,collection_or_class,attributes,lazy = False):
        if collection_or_class in self.classes:
            cls = collection_or_class
        elif collection_or_class in self.collections:
            cls = self.collections[collection_or_class]
        else:
            raise AttributeError("Unknown collection or class: %s!" % str(collection) )

        if 'constructor' in self.classes[cls]:
            obj = self.classes[cls]['constructor'](attributes,lazy = lazy)
        else:
            obj = cls(attributes,lazy = lazy,default_backend = self)
        return obj

    def get_collection_for_obj(self,obj):
        return self.get_collection_for_cls(obj.__class__)

    def get_collection_for_cls(self,cls):
        if not cls in self.classes:
            if issubclass(cls,Document) and not cls in self.classes:
                self.autoregister(cls)
            else:
                raise AttributeError("Unknown object type: %s" % cls.__name__)
        collection = self.classes[cls]['collection']
        return collection

    def get_cls_for_collection(self,collection):
        for cls,params in self.classes.items():
            if params['collection'] == collection:
                return cls
        raise AttributeError("Unknown collection: %s" % collection)

    @abc.abstractmethod
    def save(self,obj,cache = None):
        pass

    @abc.abstractmethod
    def get(self,cls,properties):
        pass

    @abc.abstractmethod
    def delete(self,obj):
        pass        

    @abc.abstractmethod
    def filter(self,cls,properties,sort_by = None,limit = None,offset = None):
        pass

