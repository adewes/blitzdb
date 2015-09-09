
def get_value(obj,key,create = False):
    key_fragments = key.split(".")
    current_dict = obj
    for key_fragment in key_fragments:
        if create and not key_fragment in current_dict:
            current_dict[key_fragment] = {}
        current_dict = current_dict[key_fragment]
    return current_dict

def set_value(obj,key,value,overwrite = True):
    key_fragments = key.split('.')
    current_dict = obj
    for key_fragment in key_fragments[:-1]:
        if not key_fragment in current_dict:
            current_dict[key_fragment] = {}
        current_dict = current_dict[key_fragment]
    if (not overwrite) and key_fragments[-1] in current_dict:
        return current_dict[key_fragments[-1]]
    current_dict[key_fragments[-1]] = value
    return current_dict[key_fragments[-1]]

def delete_value(obj,key):
    key_fragments = key.split('.')
    current_dict = obj
    for key_fragment in key_fragments[:-1]:
        if not key_fragment in current_dict:
            return
        current_dict = current_dict[key_fragment]

    if key_fragments[-1] in current_dict:
        del current_dict[key_fragments[-1]]
