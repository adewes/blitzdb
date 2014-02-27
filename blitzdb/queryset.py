import abc

class QuerySet(object):

    """
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self,backend,cls):
        self.cls = cls
        self.backend = backend

    @abc.abstractmethod
    def __getitem__(self,i):
        pass

    @abc.abstractmethod
    def delete(self):
        pass

    @abc.abstractmethod
    def filter(self,*args,**kwargs):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass

    @abc.abstractmethod
    def __ne__(self,other):
        pass
    
    @abc.abstractmethod
    def __eq__(self,other):
        pass
