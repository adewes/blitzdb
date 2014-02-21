from blitzdb.backends.file.utils import JsonEncoder
import json
import cjson
import cPickle
import marshal

"""
Serializers take a Python object and return a string representation of it.
BlitzDB currently supports several differen JSON serializers,
as well as a cPickle and marshal serializer.
"""

class JsonSerializer(object):

    @classmethod
    def serialize(cls,data):
        return json.dumps(data,cls = JsonEncoder)

    @classmethod
    def deserialize(cls,data):
        return json.loads(data)

class CJsonSerializer(object):

    @classmethod
    def serialize(cls,data):
        return cjson.encode(data)

    @classmethod
    def deserialize(cls,data):
        return cjson.decode(data)

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