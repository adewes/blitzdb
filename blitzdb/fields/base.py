
class BaseField(object):
    
    def __init__(self,key = None,
                 nullable = True,
                 unique = False,
                 default = None,
                 primary_key = False,
                 server_default = None,
                 indexed = False):
        self.key = key
        self.nullable = nullable
        self.indexed = indexed
        self.primary_key = primary_key
        self.default = default
        self.server_default = server_default
        self.unique = unique

