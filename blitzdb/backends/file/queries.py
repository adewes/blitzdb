"""Query operators for the file backend."""
import operator
import re

import six

if six.PY3:
    from functools import reduce


def boolean_operator_query(boolean_operator):
    """Generate boolean operator checking function."""
    def _boolean_operator_query(expressions):
        """Apply boolean operator to expressions."""
        def _apply_boolean_operator(query_function, expressions=expressions):
            """Return if expressions with boolean operator are satisfied."""
            compiled_expressions = [compile_query(e) for e in expressions]
            return reduce(
                boolean_operator,
                [e(query_function) for e in compiled_expressions]
            )

        return _apply_boolean_operator
    return _boolean_operator_query


def filter_query(key, expression):
    """Filter documents with a key that satisfies an expression."""
    if (isinstance(expression, dict)
            and len(expression) == 1
            and list(expression.keys())[0].startswith('$')):
        compiled_expression = compile_query(expression)
    elif callable(expression):
        def _filter(index, expression=expression):
            result = [store_key
                      for value, store_keys in index.get_index().items()
                      if expression(value)
                      for store_key in store_keys]
            return result
        compiled_expression = _filter
    else:
        compiled_expression = expression

    def _get(query_function, key=key, expression=compiled_expression):
        """Get document key and check against expression."""
        return query_function(key, expression)

    return _get


def not_query(expression):
    """Apply logical not operator to expression."""
    compiled_expression = compile_query(expression)

    def _not(index, expression=compiled_expression):
        """Return store key for documents that satisfy expression."""
        all_keys = index.get_all_keys()
        returned_keys = expression(index)
        return [key for key in all_keys if key not in returned_keys]

    return _not


def comparison_operator_query(comparison_operator):
    """Generate comparison operator checking function."""
    def _comparison_operator_query(expression):
        """Apply binary operator to expression."""
        def _apply_comparison_operator(index, expression=expression):
            """Return store key for documents that satisfy expression."""
            ev = expression() if callable(expression) else expression
            return [
                store_key
                for value, store_keys
                in index.get_index().items()
                if comparison_operator(value, ev)
                for store_key in store_keys
            ]
        return _apply_comparison_operator
    return _comparison_operator_query


def exists_query(expression):
    """Check that documents have a key that satisfies expression."""
    def _exists(index, expression=expression):
        """Return store key for documents that satisfy expression."""
        ev = expression() if callable(expression) else expression
        if ev:
            return [
                store_key
                for store_keys
                in index.get_index().values()
                for store_key in store_keys
            ]
        else:
            return index.get_undefined_keys()

    return _exists


def regex_query(expression):
    """Apply regular expression to result of expression."""
    def _regex(index, expression=expression):
        """Return store key for documents that satisfy expression."""
        pattern = re.compile(expression)
        return [
            store_key
            for value, store_keys
            in index.get_index().items()
            if (isinstance(value, six.string_types)
                and re.match(pattern, value))
            for store_key in store_keys
        ]

    return _regex


def all_query(expression):
    """Match arrays that contain all elements in the query."""
    def _all(index, expression=expression):
        """Return store key for documents that satisfy expression."""
        ev = expression() if callable(expression) else expression
        try:
            iter(ev)
        except TypeError:
            raise AttributeError('$all argument must be an iterable!')

        hashed_ev = [index.get_hash_for(v) for v in ev]
        store_keys = set([])

        if len(hashed_ev) == 0:
            return []
        store_keys = set(index.get_keys_for(hashed_ev[0]))
        for value in hashed_ev[1:]:
            store_keys &= set(index.get_keys_for(value))
        return list(store_keys)

    return _all


def elemMatch_query(expression):
    """Select documents if element in array field matches all conditions."""
    def _elemMatch(index, expression=expression):
        """Raise exception since this operator is not implemented yet."""
        raise ValueError(
            '$elemMatch query is currently not supported by file backend!')

    return _elemMatch


def in_query(expression):
    """Match any of the values that exist in an array specified in query."""
    def _in(index, expression=expression):
        """Return store key for documents that satisfy expression."""
        ev = expression() if callable(expression) else expression
        try:
            iter(ev)
        except TypeError:
            raise AttributeError('$in argument must be an iterable!')
        hashed_ev = [index.get_hash_for(v) for v in ev]
        store_keys = set()

        for value in hashed_ev:
            store_keys |= set(index.get_keys_for(value))

        return list(store_keys)

    return _in

def compile_query(query):
    """Compile each expression in query recursively."""
    if isinstance(query, dict):
        expressions = []
        for key, value in query.items():
            if key.startswith('$'):
                if key not in query_funcs:
                    raise AttributeError('Invalid operator: {0}'.format(key))
                expressions.append(query_funcs[key](value))
            else:
                expressions.append(filter_query(key, value))
        if len(expressions) > 1:
            return boolean_operator_query(operator.and_)(expressions)
        else:
            return (
                expressions[0]
                if len(expressions)
                else lambda query_function: query_function(None, None)
            )
    else:
        return query

query_funcs = {
    '$regex': regex_query,
    '$exists': exists_query,
    '$and': boolean_operator_query(operator.and_),
    '$all': all_query,
    '$elemMatch': elemMatch_query,
    '$or': boolean_operator_query(operator.or_),
    '$gte': comparison_operator_query(operator.ge),
    '$lte': comparison_operator_query(operator.le),
    '$gt': comparison_operator_query(operator.gt),
    '$lt': comparison_operator_query(operator.lt),
    '$ne': comparison_operator_query(operator.ne),
    '$not': not_query,
    '$in': in_query,
}
