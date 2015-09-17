from blitzdb.fields import BaseField

class ForeignKeyField(BaseField):

    """
    The ManyToManyProxy should support the following operations:

    - Retrieve related documents from the database
    - Append new documents to the relation
    - Remove documents from the relation
    """

    def __init__(self,related,backref = None,ondelete = None,*args,**kwargs):
        super(ForeignKeyField,self).__init__(*args,**kwargs)
        self.related = related
        self.backref = backref
        self.ondelete = ondelete
