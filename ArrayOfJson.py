import json
import pandas as pd

# Function to flatten JSON and save as CSV
def flatten_and_save_to_csv(input_file, output_file):
    flattened_data = []

    # Read JSON data from file
    with open(input_file, 'r') as f:
        json_data = json.load(f)

    for record in json_data:
        event_name = record.get("event")
        event_date = record.get("date")

        for group in record.get("group", []):
            group_id = group.get("group_id")
            group_name = group.get("group_name")

            for member in group.get("member", []):
                flattened_data.append({
                    "event": event_name,
                    "date": event_date,
                    "group_id": group_id,
                    "group_name": group_name,
                    "member_id": member.get("member_id"),
                    "member_name": member.get("name")
                })

    # Convert to a DataFrame and save as CSV
    df = pd.DataFrame(flattened_data)
    df.to_csv(output_file, index=False)
    print(f"CSV file saved: {output_file}")

# Input JSON file and output CSV file
input_file = "input_data.json"
output_file = "flattened_data.csv"

# Call the function
flatten_and_save_to_csv(input_file, output_file)
