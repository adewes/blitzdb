
class BaseField(object):
    
    def __init__(self,key = None,
                 nullable = True,
                 unique = False,
                 default = None,
                 server_default = None,
                 indexed = False):
        self.key = key
        self.nullable = nullable
        self.indexed = indexed
        self.default = default
        self.server_default = server_default
        self.unique = unique

