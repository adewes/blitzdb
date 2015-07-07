from __future__ import absolute_import

import pytest
import tempfile

from .fixtures import *
from ..backends.file import NonUnique
from blitzdb.tests.helpers.movie_data import Actor


def test_nonunique_file_backend_index(file_backend):
    file_backend.create_index(Actor, fields={'yob': 1})
    actor1 = Actor({'yob': 1})
    actor1.save(backend)
    actor2 = Actor({'yob': 1})
    actor2.save(backend)
    file_backend.commit()
    assert actor1.pk != actor2.pk


def test_unique_file_backend_index(file_backend):
    file_backend.create_index(Actor, fields={'num_films': 1}, unique=True)
    actor1 = Actor({'num_films': 1})
    actor1.save(backend)
    actor2 = Actor({'num_films': 1})
    actor2.save(backend)
    with pytest.raises(NonUnique):
        file_backend.commit()
