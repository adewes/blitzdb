from .base import BaseField

class ManyToManyField(BaseField):

    """
    """

    def __init__(self,related,backref = None,*args,**kwargs):
        super(ManyToManyField,self).__init__(*args,**kwargs)
        self.related = related
        self.backref = backref
