import json
import pandas as pd

# Function to flatten JSON and save as CSV
def flatten_and_save_to_csv(input_file, output_file):
    flattened_data = []

    with open(input_file, 'r') as f:
        data = f.read().strip()  # Read all the data at once

        # Split the records by closing and opening braces (manually fix the structure)
        records = data.split('}{')
        
        # Fix the first and last record to ensure proper JSON format
        if records:
            records[0] = '{' + records[0]  # Add the opening brace to the first record
            records[-1] = records[-1] + '}'  # Add the closing brace to the last record

            # Process each record individually
            for record in records:
                try:
                    # Parse the individual JSON record
                    json_data = json.loads(record)

                    event_name = json_data.get("event")
                    event_date = json_data.get("date")

                    if "group" in json_data and json_data["group"]:
                        for group in json_data.get("group", []):
                            group_id = group.get("group_id")
                            group_name = group.get("group_name")

                            if "member" in group and group["member"]:
                                for member in group.get("member", []):
                                    flattened_data.append({
                                        "event": event_name,
                                        "date": event_date,
                                        "group_id": group_id,
                                        "group_name": group_name,
                                        "member_id": member.get("member_id"),
                                        "member_name": member.get("name")
                                    })
                            else:
                                flattened_data.append({
                                    "event": event_name,
                                    "date": event_date,
                                    "group_id": group_id,
                                    "group_name": group_name,
                                    "member_id": None,
                                    "member_name": None
                                })

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

    # Convert to DataFrame and save as CSV
    if flattened_data:
        df = pd.DataFrame(flattened_data)
        df.to_csv(output_file, index=False)
        print(f"CSV file saved: {output_file}")
    else:
        print("No valid data to save.")

# Input JSON file and output CSV file
input_file = "input_data.json"  # Replace with your input JSON file
output_file = "flattened_data.csv"  # Replace with your desired output CSV file

# Call the function to flatten JSON and save as CSV
flatten_and_save_to_csv(input_file, output_file)
