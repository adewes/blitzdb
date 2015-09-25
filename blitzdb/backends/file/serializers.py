from blitzdb.backends.file.utils import JsonEncoder
import json
import sys
import six

if six.PY3:
    import pickle as cPickle
else:
    import cPickle

"""
Serializers take a Python object and return a string representation of it.
BlitzDB currently supports several differen JSON serializers,
as well as a cPickle serializer.
"""


class JsonSerializer(object):

    @classmethod
    def serialize(cls, data):
        if six.PY3:
            if isinstance(data, bytes):
                return json.dumps(data.decode('utf-8'), cls=JsonEncoder,ensure_ascii = False).encode('utf-8')
            else:
                return json.dumps(data, cls=JsonEncoder,ensure_ascii = False).encode('utf-8')
        else:
            return json.dumps(data, cls=JsonEncoder,ensure_ascii = False).encode('utf-8')

    @classmethod
    def deserialize(cls, data):
        if six.PY3:
            return json.loads(data.decode('utf-8'))
        else:
            return json.loads(data.decode('utf-8'))


class PickleSerializer(object):

    @classmethod
    def serialize(cls, data):
        return cPickle.dumps(data, cPickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls, data):
        return cPickle.loads(data)

try:
    import cjson

    class CJsonSerializer(object):

        @classmethod
        def serialize(cls, data):
            return cjson.encode(data)

        @classmethod
        def deserialize(cls, data):
            return cjson.decode(data)

except ImportError:
    pass