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


from blitzdb.backends.file.btree import *

def test_btree_basics():
    tree = BTree(2)
    assert tree.size() == 0
    tree.insert(1)
    assert tree.has(1) == True
    tree.insert(2)
    tree.insert(3)
    assert tree.has(4) == False
    assert tree.size() == 3
    assert tree.has(3) == True
    tree.remove(3)
    assert tree.has(3) == False
    tree.insert("apple")
    tree.insert({"three":3})
    assert tree.has("apple") == True

    tree = BTree()
    tree.load_list(["b", "l", "i", "t", "z", "d", "b"])
    assert tree.has("b") == True
    assert tree.size() == 7