
class BaseField(object):
    
    def __init__(self,key = None,nullable = True,indexed = False,**kwargs):
        self.key = key
        self.nullable = nullable
        self.indexed = indexed
        self.opts = kwargs

