from .base import BaseField

class ManyToManyField(BaseField):

    """
    """

    def __init__(self,
                 related,
                 field = None,
                 related_field = None,
                 backref = None,
                 ondelete = 'CASCADE',
                 *args,**kwargs):
        super(ManyToManyField,self).__init__(*args,**kwargs)
        self.related = related
        self.backref = backref
        self.field = field
        self.related_field = related_field
        self.ondelete = ondelete
