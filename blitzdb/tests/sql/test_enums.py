import pytest
from sqlalchemy.exc import IntegrityError

from blitzdb.fields import EnumField
from ..helpers.movie_data import Document


class MyActor(Document):

    best_genre = EnumField(enums = ('action','sci-fi','romance','comedy'))

def test_basics(backend):

    backend.register(MyActor)
    backend.init_schema()
    backend.create_schema()

    al_pacino = MyActor({'name' : 'Al Pacino','best_genre' : 'action'})
    backend.save(al_pacino)
    backend.commit()
    al_pacino.revert(backend)
    assert al_pacino.best_genre == 'action'
    al_pacino.best_genre = 'foobar'

    with pytest.raises(IntegrityError):
        backend.save(al_pacino)
        backend.commit()
