import json
import csv
from pathlib import Path

def process_and_flatten_json_to_csv(input_file, output_file):
    # Read and parse the JSON records
    with open(input_file, "r") as f:
        raw_data = f.read()
        # Fix formatting: Wrap in array brackets if necessary
        json_data = json.loads(f"[{raw_data.replace('}{', '},{')}]")

    # Flatten JSON data
    flattened_data = []
    for record in json_data:
        event = record.get("event", "")
        date = record.get("date", "")
        for group in record.get("group", []):
            group_id = group.get("group_id", "")
            group_name = group.get("group_name", "")
            members = group.get("member", [])
            if not members:  # Handle empty member list
                flattened_data.append({
                    "event": event,
                    "date": date,
                    "group_id": group_id,
                    "group_name": group_name,
                    "member_id": "",
                    "member_name": ""
                })
            for member in members:
                flattened_data.append({
                    "event": event,
                    "date": date,
                    "group_id": group_id,
                    "group_name": group_name,
                    "member_id": member.get("member_id", ""),
                    "member_name": member.get("name", "")
                })

    # Write flattened data to CSV
    csv_headers = ["event", "date", "group_id", "group_name", "member_id", "member_name"]
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(flattened_data)

# File paths
input_path = "nested_records.json"  # Input JSON file
output_path = "flattened_output.csv"  # Output CSV file

# Call the function
process_and_flatten_json_to_csv(input_path, output_path)
