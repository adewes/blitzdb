from .base import BaseField

class DateTimeField(BaseField):
    

    def __init__(self,auto_now = False,auto_now_add = False,*args,**kwargs):
        super(DateTimeField,self).__init__(*args,**kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
