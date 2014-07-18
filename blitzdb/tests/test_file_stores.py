import pytest
import tempfile
import subprocess

from blitzdb.backends.file import TransactionalStore


@pytest.fixture(scope="function")
def transactional_store(request):

    tmpdir = tempfile.mkdtemp()

    def finalizer():
        subprocess.call(["rm", "-rf", tmpdir])

    request.addfinalizer(finalizer)
    return TransactionalStore({'path': tmpdir})


def test_transactional_store_save(transactional_store):

    store = transactional_store
    store.autocommit = True

    blob1 = "abdsfdsfdsfsfdsfsad"
    blob2 = "sdfbvcdfgfsfdsssd"
    blob3 = "sdfsdfdsf dsfsdfs fsfsf sfsf"

    store.store_blob(blob1, "key1")
    store.store_blob(blob2, "key2")
    store.store_blob(blob3, "key3")

    assert store.get_blob("key1") == blob1
    assert store.get_blob("key2") == blob2
    assert store.get_blob("key3") == blob3

    store.store_blob(blob2, "key1")
    store.delete_blob("key2")
    assert store.get_blob("key1") == blob2
    store.store_blob(blob2, "key2")

    store.delete_blob("key1")

    with pytest.raises(KeyError):
        store.delete_blob("key1")  # <<<---

    with pytest.raises(KeyError):
        store.get_blob("key1")

    assert store.get_blob("key2") == blob2

    store.delete_blob("key2")

    assert store.get_blob("key3") == blob3
