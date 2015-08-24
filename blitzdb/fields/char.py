from .base import BaseField

class CharField(BaseField):
    
    def __init__(self,max_length = None,*args,**kwargs):
        self.max_length = max_length
        super(CharField,self).__init__(*args,**kwargs)
