from .base import BaseField

class CharField(BaseField):
    
    def __init__(self,max_length = None,**kwargs):
        self.max_length = max_length
        super(CharField,self).__init__(**kwargs)
