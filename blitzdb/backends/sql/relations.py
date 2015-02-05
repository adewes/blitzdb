

class ManyToManyProxy(object):

    def __init__(self,instance,descriptor):
        self.instance = instance
        self.descriptor = descriptor

class ManyToMany(object):

    def __init__(self,cls,name = None,qualifier = None):
        self._cls = cls
        self._name = name
        self._qualifier = qualifier

    def __get__(self,instance,owner):
        if instance == None:
            raise AttributeError("ManyToMany descriptor must be called on class instance!")
        return ManyToManyProxy(instance,self)

    def __set__(self,instance,value):
        raise AttributeError("Cannot set descriptor!")

class ForeignKeyProxy(object):

    def __init__(self,instance,descriptor):
        self.instance = instance
        self.descritor = descriptor

class ForeignKey(object):

    def __init__(self,cls):
        self._cls = cls

    def __get__(self,instance,owner):
        if instance == None:
            raise AttributeError("ForeignKey descriptor must be called on class instance!")
        return ForeignKeyProxy(instance,self)

class IndexProxy(object):

    def __init__(self,instance,descriptor):
        self.instance = instance
        self.descriptor = descriptor

class Index(object):

    def __init__(self,cls):
        self._cls = cls

    def __get__(self,instance,owner):
        if instance == None:
            raise AttributeError("Index descriptor must be called on class instance!")
        return IndexProxy(instance,self)
