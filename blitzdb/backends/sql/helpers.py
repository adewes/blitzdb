
def get_value(obj,key):
    key_fragments = key.split(".")
    current_dict = obj
    for key_fragment in key_fragments:
        current_dict = current_dict[key_fragment]
    return current_dict

def set_value(obj,key,value):
    key_fragments = key.split('.')
    current_dict = obj
    for key_fragment in key_fragments[:-1]:
        if not key_fragment in current_dict:
            current_dict[key_fragment] = {}
        current_dict = current_dict[key_fragment]
    current_dict[key_fragments[-1]] = value
