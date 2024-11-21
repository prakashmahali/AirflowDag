import json
import pandas as pd

# Function to flatten JSON and save as CSV
def flatten_and_save_to_csv(input_file, output_file):
    flattened_data = []

    # Read the JSON data from file
    with open(input_file, 'r') as f:
        json_data = json.load(f)

    for record in json_data:
        event_name = record.get("event")
        event_date = record.get("date")

        # Check if the 'group' key exists and is not empty
        if "group" in record and record["group"]:
            for group in record.get("group", []):
                group_id = group.get("group_id", None)
                group_name = group.get("group_name", None)

                # Check if the 'member' list exists and is not empty
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
                    # Handle cases where 'member' list is empty
                    flattened_data.append({
                        "event": event_name,
                        "date": event_date,
                        "group_id": group_id,
                        "group_name": group_name,
                        "member_id": None,
                        "member_name": None
                    })
        else:
            # Handle cases where 'group' list is empty
            flattened_data.append({
                "event": event_name,
                "date": event_date,
                "group_id": None,
                "group_name": None,
                "member_id": None,
                "member_name": None
            })

    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(flattened_data)
    df.to_csv(output_file, index=False)
    print(f"CSV file saved: {output_file}")

# Input JSON file and output CSV file
input_file = "input_data.json"  # Replace with your input JSON file
output_file = "flattened_data.csv"  # Replace with your desired output CSV file

# Call the function to flatten JSON and save as CSV
flatten_and_save_to_csv(input_file, output_file)
