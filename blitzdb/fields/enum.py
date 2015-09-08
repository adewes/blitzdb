from .base import BaseField

class EnumField(BaseField):
    
    def __init__(self,enums,*args,**kwargs):
        self.enums = enums
        super(EnumField,self).__init__(*args,**kwargs)
