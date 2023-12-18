"""
Module for handling JSON data and generating Python classes from it.
"""
import json


def _build_jmespath_queries(data, current_path=''):
    """
    Recursively build JMESPath queries for JSON data.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{current_path}.{key}"
            yield from _build_jmespath_queries(value, new_path)
    elif isinstance(data, list) and data:
        yield from _build_jmespath_queries(data[0], f"{current_path}[]")
    else:
        yield current_path


def create_parse_map(input_json_file, output_txt_file):
    """
    Read JSON file, build parse map, and save to text file.
    """
    with open(input_json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    queries = list(_build_jmespath_queries(data))

    mapping = {query.split('.')[-1]: query for query in queries}

    with open(output_txt_file, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, indent=4)


def _type_inference(value, name):
    """
    Infer the Python type based on the value.
    """
    if isinstance(value, str):
        if value.startswith("\u00a3"):
            return 'int'
        return 'str'
    if isinstance(value, int):
        return 'int'
    if isinstance(value, float):
        return 'float'
    if isinstance(value, list):
        if value and isinstance(value[0], dict):
            return name.capitalize()
        return 'List[Any]'
    if isinstance(value, dict):
        return name.capitalize()
    if isinstance(value, bool):
        return 'bool'
    if value is None:
        return 'Optional[Any]'
    return 'Any'


def _generate_class(data, class_name):
    """
    Recursively generate a Python class definition from a dictionary.
    """
    class_def = [f"class {class_name}:"]

    if not data:
        class_def.append("    pass")
        return class_def

    for key, value in data.items():
        data_type = _type_inference(value, key)

        if isinstance(value, list) and value and isinstance(value[0], dict):
            nested_list_class_def = _generate_class(value[0], data_type)
            class_def = nested_list_class_def + ["\n"] + class_def
            data_type = f"List[{data_type}]"

        elif isinstance(value, dict):
            nested_class_def = _generate_class(value, data_type)
            class_def = nested_class_def + ["\n"] + class_def

        class_def.append(f"    {key}: {data_type}")

    return class_def


def generate_class_from_json(json_file_path: str, output_file_path: str, class_name: str):
    """
    Generate Python classes with typed properties based on a JSON file and save them to a .py file.
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    class_code = '\n'.join(_generate_class(data, class_name))

    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write("from typing import List, Optional, Any\n\n")
        f.write(class_code)
        f.write("\n")
        

def cleanNoneValue(data: dict) -> dict:
    out = {}
    for k in data.keys():
        if data[k] is not None:
            out[k] = data[k]
    return out