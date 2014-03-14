from blitzdb.backends.file.utils import JsonEncoder
import json
import sys
import marshal
import six

if six.PY3:
    import pickle as cPickle
else:
    import cPickle

"""
Serializers take a Python object and return a string representation of it.
BlitzDB currently supports several differen JSON serializers,
as well as a cPickle and marshal serializer.
"""

class JsonSerializer(object):

    @classmethod
    def serialize(cls,data):
        if six.PY3:
            if isinstance(data, bytes):
                return json.dumps(data.decode('utf-8'),cls = JsonEncoder).encode('utf-8')
            else:
                return json.dumps(data,cls = JsonEncoder).encode('utf-8')
        else:
            return json.dumps(data,cls = JsonEncoder)

    @classmethod
    def deserialize(cls,data):
        if six.PY3:
            return json.loads(data.decode('utf-8'))
        else:
            return json.loads(data)

class PickleSerializer(object):

    @classmethod
    def serialize(cls,data):
        return cPickle.dumps(data,cPickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls,data):
        return cPickle.loads(data)


class MarshalSerializer(object):

    @classmethod
    def serialize(cls,data):
        return marshal.dumps(data)

    @classmethod
    def deserialize(cls,data):
        return marshal.loads(data)

try:
    import cjson

    class CJsonSerializer(object):

        @classmethod
        def serialize(cls,data):
            return cjson.encode(data)

        @classmethod
        def deserialize(cls,data):
            return cjson.decode(data)

except ImportError:
    #we fall back to the normal JSON serializer
    sys.stderr.write("Warning: cjson could not be imported, CJsonSerializer will not be available.\n")
