"""File backend index."""
import copy

from collections import defaultdict

from blitzdb.backends.base import NotInTransaction
from blitzdb.backends.file.serializers import PickleSerializer as Serializer
from blitzdb.backends.file.utils import JsonEncoder
from blitzdb.backends.file.queryset import QuerySet

class NonUnique(BaseException):
    """Index uniqueness constraint violated"""
    pass


class Index(object):

    """File backend index.

    An index accepts key/value pairs and stores them so that they can be
    efficiently retrieved.

    :param params: Index parameters such as id and primary key
    :type params: dict
    :param serializer: Used to encode data before storing it.
    :type serialize: object
    :param dserializer: Used to decode date after retrieving it.
    :type deserializer: object
    :param store: Where the blobs are stored
    :type store: object

    """

    def __init__(self, params, serializer, deserializer, store=None, unique=False):
        """Initalize internal state."""
        self._params = params
        self._store = store
        self._serializer = serializer
        self._deserializer = deserializer
        self._splitted_key = self.key.split('.')
        self._unique = unique

        self._index = None
        self._reverse_index = None
        self._undefined_keys = None
        self.clear()

        if store:
            self.ephemeral = False
            self.loaded = self.load_from_store()
        else:
            self.ephemeral = True
            self.loaded = False

    def clear(self):
        """Clear index."""
        self._index = defaultdict(list)
        self._reverse_index = defaultdict(list)
        self._undefined_keys = {}

    @property
    def key(self):
        """Return key parameter.

        An index will be created by default in which the key is the document
        primary key, but custom indices can be created for any property
        (including nested ones).

        :return: primary key
        :rtype: str

        """
        return self._params['key']

    def get_value(self, attributes,key = None):
        """Get value to be indexed from document attributes.

        :param attributes: Document attributes
        :type attributes: dict
        :return: Value to be indexed
        :rtype: object

        """

        value = attributes
        if key is None:
            key = self._splitted_key

        # A splitted key like 'a.b.c' goes into nested properties
        # and the value is retrieved recursively
        for i,elem in enumerate(key):
            if isinstance(value, (list,tuple)):
                #if this is a list, we return all matching values for the given list items
                return [self.get_value(v,key[i:]) for v in value]
            else:
                value = value[elem]

        return value

    def save_to_store(self):
        """Save index to store.

        :raise AttributeError: If no datastore is defined

        """
        if not self._store:
            raise AttributeError('No datastore defined!')
        saved_data = self.save_to_data(in_place=True)
        data = Serializer.serialize(saved_data)
        self._store.store_blob(data, 'all_keys_with_undefined')

    def get_all_keys(self):
        """Get all keys indexed.

        :return: All keys
        :rtype: list(str)

        """
        all_keys = []
        for keys in self._index.values():
            all_keys.extend(keys)
        return all_keys

    def get_index(self):
        """Get copy of the internal index structure.

        :return: Internal index structure
        :rtype: dict(str)

        """
        return copy.deepcopy(self._index)

    def load_from_store(self):
        """Load index from store.

        :return: Whether index was correctly loaded or not
        :rtype: bool
        :raise AttributeError: If no datastore is defined

        """
        if not self._store:
            raise AttributeError('No datastore defined!')
        if self._store.has_blob('all_keys'):
            data = Serializer.deserialize(self._store.get_blob('all_keys'))
            self.load_from_data(data)
            return True
        elif self._store.has_blob('all_keys_with_undefined'):
            blob = self._store.get_blob('all_keys_with_undefined')
            data = Serializer.deserialize(blob)
            self.load_from_data(data, with_undefined=True)
            return True
        else:
            return False

    def sort_keys(self, keys, order=QuerySet.ASCENDING):
        """Sort keys.

        Keys are sorted based on the value they are indexing.

        :param keys: Keys to be sorted
        :type keys: list(str)
        :param order: Order criteri (asending or descending)
        :type order: int
        :return: Sorted keys
        :rtype: list(str)
        :raise ValueError: If invalid order value is passed

        """
        # to do: check that all reverse index values are unambiguous
        missing_keys = [
            key
            for key in keys
            if not len(self._reverse_index[key])
        ]
        keys_and_values = [
            (key, self._reverse_index[key][0])
            for key in keys
            if key not in missing_keys
        ]
        sorted_keys = [
            kv[0]
            for kv in sorted(
                keys_and_values,
                key=lambda x: x[1],
                reverse=True if order == QuerySet.DESCENDING else False)
        ]
        if order == QuerySet.ASCENDING:
            return missing_keys + sorted_keys
        elif order == QuerySet.DESCENDING:
            return sorted_keys + missing_keys
        else:
            raise ValueError('Unexpected order value: {0:d}'.format(order))

    def save_to_data(self, in_place=False):
        """Save index to data structure.

        :param in_place: Do not copy index value to a new list object
        :type in_place: bool
        :return: Index data structure
        :rtype: list

        """
        if in_place:
            return [
                list(self._index.items()),
                list(self._undefined_keys.keys())
            ]
        return (
            [(key, values[:]) for key, values in self._index.items()],
            list(self._undefined_keys.keys()),
        )

    def load_from_data(self, data, with_undefined=False):
        """Load index structure.

        :param with_undefined: Load undefined keys as well
        :type with_undefined: bool

        """
        if with_undefined:
            defined_values, undefined_values = data
        else:
            defined_values = data
            undefined_values = None
        self._index = defaultdict(list, defined_values)
        self._reverse_index = defaultdict(list)
        for key, values in self._index.items():
            for value in values:
                self._reverse_index[value].append(key)
        if undefined_values:
            self._undefined_keys = {key: True for key in undefined_values}
        else:
            self._undefined_keys = {}

    def get_hash_for(self, value):
        """Get hash for a given value.

        :param value: The value to be indexed
        :type value: object
        :return: Hashed value
        :rtype: str

        """
        if isinstance(value,dict) and '__ref__' in value:
            return self.get_hash_for(value['__ref__'])
        serialized_value = self._serializer(value)
        if isinstance(serialized_value, dict):
            # Hash each item and return the hash of all the hashes
            return hash(frozenset([
                self.get_hash_for(x)
                for x in serialized_value.items()
            ]))
        elif isinstance(serialized_value, (list,tuple)):
            # Hash each element and return the hash of all the hashes
            return hash(tuple([
                self.get_hash_for(x) for x in serialized_value
            ]))
        return value

    def get_keys_for(self, value):
        """Get keys for a given value.

        :param value: The value to look for
        :type value: object
        :return: The keys for the given value
        :rtype: list(str)

        """
        if callable(value):
            return value(self)
        hash_value = self.get_hash_for(value)
        return self._index[hash_value][:]

    def get_undefined_keys(self):
        """Get undefined keys.

        :return: Undefined keys
        :rtype: list(str)

        """
        return self._undefined_keys.keys()

    # The following two operations change the value of the index

    def add_hashed_value(self, hash_value, store_key):
        """Add hashed value to the index.

        :param hash_value: The hashed value to be added to the index
        :type hash_value: str
        :param store_key: The key for the document in the store
        :type store_key: object

        """
        if self._unique and hash_value in self._index:
            raise NonUnique('Hash value {0} already in index'.format(hash_value))
        if store_key not in self._index[hash_value]:
            self._index[hash_value].append(store_key)
        if hash_value not in self._reverse_index[store_key]:
            self._reverse_index[store_key].append(hash_value)

    def add_key(self, attributes, store_key):
        """Add key to the index.

        :param attributes: Attributes to be added to the index
        :type attributes: dict(str)
        :param store_key: The key for the document in the store
        :type store_key: str

        """
        undefined = False
        try:
            value = self.get_value(attributes)
        except (KeyError, IndexError):
            undefined = True

        # We remove old values in _reverse_index
        self.remove_key(store_key)
        if not undefined:
            if isinstance(value, (list,tuple)):
                # We add an extra hash value for the list itself
                # (this allows for querying the whole list)
                values = value
                hash_value = self.get_hash_for(value)
                self.add_hashed_value(hash_value, store_key)
            else:
                values = [value]

            for value in values:
                hash_value = self.get_hash_for(value)
                self.add_hashed_value(hash_value, store_key)
        else:
            self.add_undefined(store_key)

    def add_undefined(self, store_key):
        """Add undefined key to the index.

        :param store_key: The key for the document in the store
        :type store_key: str

        """
        self._undefined_keys[store_key] = True

    def remove_key(self, store_key):
        """Remove key from the index.

        :param store_key: The key for the document in the store
        :type store_key: str

        """
        if store_key in self._undefined_keys:
            del self._undefined_keys[store_key]
        if store_key in self._reverse_index:
            for value in self._reverse_index[store_key]:
                self._index[value].remove(store_key)
            del self._reverse_index[store_key]


class TransactionalIndex(Index):

    """This class adds transaction support to the Index class."""

    def __init__(self, *args, **kwargs):
        """Initialize internal state."""
        super(TransactionalIndex, self).__init__(*args, **kwargs)
        self._in_transaction = False

        self._add_cache = None
        self._reverse_add_cache = None
        self._remove_cache = None
        self._init_cache()

    def _init_cache(self):
        """Initialize cache."""
        self._add_cache = defaultdict(list)
        self._reverse_add_cache = defaultdict(list)
        self._undefined_cache = {}
        self._remove_cache = {}

    def begin(self):
        """Begin transaction.

        This will commit the last transaction before starting a new one.

        """
        self.commit()

    def commit(self):
        """Commit current transaction."""
        if (not self._add_cache and
                not self._remove_cache and
                not self._undefined_cache):
            return

        for store_key, hash_values in self._add_cache.items():
            for hash_value in hash_values:
                super(TransactionalIndex, self).add_hashed_value(
                    hash_value, store_key)
        for store_key in self._remove_cache:
            super(TransactionalIndex, self).remove_key(store_key)
        for store_key in self._undefined_cache:
            super(TransactionalIndex, self).add_undefined(store_key)
        if not self.ephemeral:
            self.save_to_store()

        self._init_cache()
        self._in_transaction = True

    def rollback(self):
        """Drop changes from current transaction."""
        if not self._in_transaction:
            raise NotInTransaction
        self._init_cache()
        self._in_transaction = False


    def add_undefined(self, store_key):
        """Add undefined key to the index.

        :param store_key: The key for the document in the store
        :type store_key: str

        """
        self._undefined_cache[store_key] = True

    def add_hashed_value(self, hash_value, store_key):
        """Add hashed value in the context of the current transaction.

        :param hash_value: The hashed value to be added to the index
        :type hash_value: str
        :param store_key: The key for the document in the store
        :type store_key: object

        """
        if hash_value not in self._add_cache[store_key]:
            self._add_cache[store_key].append(hash_value)
        if store_key not in self._reverse_add_cache[hash_value]:
            self._reverse_add_cache[hash_value].append(store_key)
        if store_key in self._remove_cache:
            del self._remove_cache[store_key]
        if store_key in self._undefined_cache:
            del self._undefined_cache[store_key]

    def remove_key(self, store_key):
        """Remove key in the context of the current transaction.

        :param store_key: The key for the document in the store
        :type store_key: str

        """
        self._remove_cache[store_key] = True
        if store_key in self._add_cache:
            for hash_value in self._add_cache[store_key]:
                self._reverse_add_cache[hash_value].remove(store_key)
            del self._add_cache[store_key]
        if store_key in self._undefined_cache:
            del self._undefined_cache[store_key]

    def get_keys_for(self, value, include_uncommitted=False):
        """Get keys for a given value.

        :param value: The value to look for
        :type value: object
        :param include_uncommitted: Include uncommitted values in results
        :type include_uncommitted: bool
        :return: The keys for the given value
        :rtype: list(str)

        """
        if not include_uncommitted:
            return super(TransactionalIndex, self).get_keys_for(value)
        else:
            keys = super(TransactionalIndex, self).get_keys_for(value)
            hash_value = self.get_hash_for(value)
            keys += self._reverse_add_cache[hash_value]
            return keys
