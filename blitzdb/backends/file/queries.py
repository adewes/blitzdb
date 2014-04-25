import six
import re

if six.PY3:
    from functools import reduce

def and_query(expressions): 

    def _and(query_function,expressions = expressions):
        compiled_expressions = [compile_query(e) for e in expressions]
        return reduce(lambda x,y: x & y,[e(query_function) for e in compiled_expressions])
        
    return _and

def or_query(expressions):

    def _or(query_function,expressions = expressions):
        compiled_expressions = [compile_query(e) for e in expressions]
        return reduce(lambda x,y: x | y,[e(query_function) for e in compiled_expressions])

    return _or

def filter_query(key,expression):

    if isinstance(expression,dict) and len(expression) == 1 and list(expression.keys())[0].startswith('$'):
        compiled_expression = compile_query(expression)
    else:
        compiled_expression = expression

    def _get(query_function,key = key,expression = compiled_expression):
        return query_function(key,expression)

    return _get

def not_query(expression):

    compiled_expression = compile_query(expression)

    def _not(index,expression = compiled_expression):
        all_keys = index.get_all_keys()
        returned_keys = expression(index)
        return [key for key in all_keys if not key in returned_keys]

    return _not

def gte_query(expression):

    def _gte(index,expression = expression):
        ev = expression() if callable(expression) else expression
        return [store_key for value,store_keys in index.get_index().items() if value >= ev for store_key in store_keys] 

    return _gte

def lte_query(expression):

    def _lte(index,expression = expression):
        ev = expression() if callable(expression) else expression
        return [store_key for value,store_keys in index.get_index().items() if value <= ev for store_key in store_keys] 

    return _lte

def gt_query(expression):

    def _gt(index,expression = expression):
        ev = expression() if callable(expression) else expression
        return [store_key for value,store_keys in index.get_index().items() if value > ev for store_key in store_keys] 

    return _gt

def lt_query(expression):

    def _lt(index,expression = expression):
        ev = expression() if callable(expression) else expression
        return [store_key for value,store_keys in index.get_index().items() if value < ev for store_key in store_keys] 

    return _lt

def ne_query(expression):

    def _ne(index,expression = expression):
        ev = expression() if callable(expression) else expression
        return [store_key for value,store_keys in index.get_index().items() if value != ev for store_key in store_keys] 

    return _ne

def exists_query(expression):

    def _exists(index,expression = expression):
        ev = expression() if callable(expression) else expression
        return [store_key for value,store_keys in index.get_index().items() for store_key in store_keys] 

    return _ne

def regex_query(expression):

    def _regex(index,expression = expression):
        pattern = re.compile(expression)
        return [store_key for value,store_keys in index.get_index().items() if re.match(pattern,value) for store_key in store_keys] 

    return _regex

def all_query(expression):

    def _all(index,expression = expression):
        ev = expression() if callable(expression) else expression
        try:
            ev_iter = iter(ev)
        except TypeError as te:
            raise AttributeError("$in argument must be an iterable!")
        hashed_ev = [index.get_hash_for(v) for v in ev]
        store_keys = set([])
        if len(hashed_ev) == 0:
            return []
        store_keys = set(index.get_keys_for(hashed_ev[0]))
        for value in hashed_ev[1:]:
            store_keys &= set(index.get_keys_for(value))
        return list(store_keys)

    return _all

def in_query(expression):

    def _in(index,expression = expression):
        ev = expression() if callable(expression) else expression
        try:
            ev_iter = iter(ev)
        except TypeError as te:
            raise AttributeError("$in argument must be an iterable!")
        hashed_ev = [index.get_hash_for(v) for v in ev]
        store_keys = set()

        for value in hashed_ev:
            store_keys |= set(index.get_keys_for(value))

        return list(store_keys)

    return _in

def compile_query(query):
    if isinstance(query,dict):
        expressions = []
        for key,value in query.items():
            if key.startswith('$'):
                if not key in query_funcs:
                    raise AttributeError("Invalid operator: %s" % key)
                expressions.append(query_funcs[key](value))
            else:
                expressions.append(filter_query(key,value))
        if len(expressions) > 1:
            return and_query(expressions) 
        else: 
            return expressions[0] if len(expressions) else lambda query_function : query_function(None,None)
    else:
        return query
    
query_funcs = {
    '$regex' : regex_query,
    '$exists' : exists_query,
    '$and' : and_query,
    '$all' : all_query,
    '$or' : or_query,
    '$gte' : gte_query,
    '$lte' : lte_query,
    '$gt' : gt_query,
    '$lt' : lt_query,
    '$ne' : ne_query,
    '$not' : not_query,
    '$in' : in_query,
}
