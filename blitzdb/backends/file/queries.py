def and_query(expressions):

    def _and(query_function,expressions = expressions):
        return reduce(lambda x,y: x & y,[e(query_function) for e in expressions])
        
    return _and

def or_query(expressions):

    def _or(query_function,expressions = expressions):
        return reduce(lambda x,y: x | y,[e(query_function) for e in expressions])

    return _or

def filter_query(key,expression):

    def _get(query_function,key = key,expression = expression):
        return query_function(key,expression)

    return _get

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
