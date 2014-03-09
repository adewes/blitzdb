
def and_query(expressions):

    def _and(collection,expressions = expressions):
        return reduce(lambda x,y: x & y,[e(collection) for e in expressions])
        
    return _and

def or_query(expressions):

    def _or(collection,expressions = expressions):
        return reduce(lambda x,y: x | y,[e(collection) for e in expressions])

    return _or

def not_query(expression):

    def _not(value,expression = expression):
        return not expression(value)

    return _not

def gte_query(expression):

    def _gte(value,expression = expression):
        ev = expression() if callable(expression) else expression
        return True if value >= ev else False

    return _gte

def lte_query(expression):

    def _lte(value,expression = expression):
        ev = expression() if callable(expression) else expression
        return True if value <= ev else False

    return _lte

def gt_query(expression):

    def _gt(value,expression = expression):
        ev = expression() if callable(expression) else expression
        return True if value > ev else False

    return _gt

def lt_query(expression):

    def _lt(value,expression = expression):
        ev = expression() if callable(expression) else expression
        return True if value < ev else False

    return _lt

def ne_query(expression):

    def _ne(value,expression = expression):
        ev = expression() if callable(expression) else expression
        return True if value != ev else False

    return _ne

def in_query(expression):

    def _in(value,expression = expression):
        ev = expression() if callable(expression) else expression
        return True if value in ev else False

    return _in

def filter_query(key,expression):

    def _get(collection,key = key,expression = expression):
        return collection.filter(key,expression)

    return _get

queries = {
    '$and' : and_query,
    '$or' : or_query,
    '$gte' : gte_query,
    '$lte' : lte_query,
    '$gt' : gt_query,
    '$lt' : lt_query,
    '$ne' : ne_query,
    '$not' : not_query,
    '$in' : in_query,
}

def compile_query(query):
    if isinstance(query,list):
        return [compile_query(q) for q in query]
    elif isinstance(query,dict):
        expressions = []
        for key,value in query.items():
            if key.startswith('$'):
                if not key in queries:
                    raise AttributeError("Invalid operator: %s" % key)
                expressions.append(queries[key](compile_query(value)))
            else:
                expressions.append(filter_query(key,compile_query(value)))
        return and_query(expressions) if len(expressions) > 1 else expressions[0]
    else:
        return query

class Collection:

    def __init__(self,objects):
        self.objects = objects

    def filter(self,key,filter_expression):
        def protected_filter(obj,key):
            try:
                if callable(filter_expression):
                    return filter_expression(obj[key])
                return obj[key] == filter_expression
            except KeyError:
                return False

        return QuerySet(set([i for i,obj in enumerate(self.objects) if protected_filter(obj,key)]))

class QuerySet():

    def __init__(self,keys):
        self.keys = keys

    def __and__(self,other):
        return QuerySet(self.keys & other.keys)

    def __or__(self,other):
        return QuerySet(self.keys | other.keys)

if __name__ == '__main__':

    objects = [
        {
            'value' : 650,
        },
        {
            'value' : 66
        },
        {
            'value' : 100
        },
        {
            'value' : 40,
            'always_include' : True
        }
    ]

    collection = Collection(objects)

    print compile_query({'$or' : 
                                [
                                    {'$and' : [{'value' : { '$gte' : 41}},{'value' : { '$lte' : 80}}]},
                                    {'value' : { '$not' : {'$lte' : 100}}},
                                    {'value' : { '$in' : [100]}},
                                    {'always_include' : True}
                                ]
                        })(collection).keys

    print compile_query({'always_include' : True})(collection).keys