import json
import csv

# Step 1: Read the JSON file and preprocess it
def preprocess_json(file_path):
    with open(file_path, 'r') as file:
        # Convert JSON-like records into a proper JSON array
        raw_data = file.read()
        formatted_data = "[" + raw_data.replace("}{", "},{") + "]"
        return json.loads(formatted_data)

# Step 2: Flatten the JSON structure
def flatten_json(data):
    flattened_data = []
    for record in data:
        event = record.get("event", "")
        date = record.get("date", "")
        groups = record.get("group", [])
        
        for group in groups:
            group_id = group.get("group_id", "")
            group_name = group.get("group_name", "")
            members = group.get("member", [])
            
            # If members exist, extract them, else write group-level data only
            if members:
                for member in members:
                    member_id = member.get("member_id", "")
                    name = member.get("name", "")
                    flattened_data.append({
                        "event": event,
                        "date": date,
                        "group_id": group_id,
                        "group_name": group_name,
                        "member_id": member_id,
                        "name": name
                    })
            else:
                flattened_data.append({
                    "event": event,
                    "date": date,
                    "group_id": group_id,
                    "group_name": group_name,
                    "member_id": "",
                    "name": ""
                })
    return flattened_data

# Step 3: Write the flattened data to CSV
def write_to_csv(flattened_data, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["event", "date", "group_id", "group_name", "member_id", "name"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(flattened_data)

# Main Execution
input_file = "input.json"  # Replace with your JSON file path
output_file = "output.csv"

# Process the JSON and write to CSV
data = preprocess_json(input_file)
flattened_data = flatten_json(data)
write_to_csv(flattened_data, output_file)

print(f"Flattened data has been written to {output_file}.")
