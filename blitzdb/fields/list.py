from .base import BaseField

class ListField(BaseField):

    """
    """

    def __init__(self,type,*args,**kwargs):
        super(ListField,self).__init__(*args,**kwargs)
        self.type = type