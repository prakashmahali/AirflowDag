import json

def replace_special_characters(json_obj):
    new_json_obj = {}
    for key, value in json_obj.items():
        new_key = key.replace('#', '_').replace('%', '_').replace(')', '_').replace('(', '_')  # Replace special characters
        new_json_obj[new_key] = value
    return new_json_obj

def replace_special_characters_in_json_file(input_file, output_file):
    with open(input_file, 'r') as f:
        json_data = json.load(f)

    modified_json_data = replace_special_characters(json_data)

    with open(output_file, 'w') as f:
        json.dump(modified_json_data, f, indent=4)

if __name__ == "__main__":
    input_file = '/path/to/input.json'
    output_file = '/path/to/output.json'
    replace_special_characters_in_json_file(input_file, output_file)
