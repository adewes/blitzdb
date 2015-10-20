
def get_value(obj,key,create = False):
    key_fragments = key.split(".")
    current_dict = obj
    last_dict = None
    last_key = None
    for key_fragment in key_fragments:
        try:
            if create and not key_fragment in current_dict:
                current_dict[key_fragment] = {}
        except TypeError:
            if last_dict:
                last_dict[last_key] = {key_fragment : {}}
                current_dict = last_dict[last_key]
            else:
                raise KeyError
        last_key = key_fragment
        last_dict = current_dict
        try:
            current_dict = current_dict[key_fragment]
        except TypeError:
            raise KeyError
    return current_dict

def set_value(obj,key,value,overwrite = True):
    key_fragments = key.split('.')
    current_dict = obj
    last_dict = None
    last_key = None
    for key_fragment in key_fragments:
        try:
            if not key_fragment in current_dict:
                current_dict[key_fragment] = {}
        except TypeError:
            if last_dict:
                last_dict[last_key] = {key_fragment : {}}
                current_dict = last_dict[last_key]
            else:
                raise
        last_dict = current_dict
        last_key = key_fragment
        current_dict = current_dict[key_fragment]

    if (not overwrite) and key_fragments[-1] in last_dict:
        return last_dict[key_fragments[-1]]

    last_dict[key_fragments[-1]] = value
    return last_dict[key_fragments[-1]]

def delete_value(obj,key):
    key_fragments = key.split('.')
    current_dict = obj
    last_dict = None
    for key_fragment in key_fragments:
        try:
            if not key_fragment in current_dict:
                return
        except TypeError:
            return
        last_dict = current_dict
        current_dict = current_dict[key_fragment]

    if key_fragments[-1] in last_dict:
        del last_dict[key_fragments[-1]]
