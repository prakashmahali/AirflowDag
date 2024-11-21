import json
import pandas as pd

# Function to flatten JSON and save as CSV
def flatten_and_save_to_csv(input_file, output_file):
    flattened_data = []

    # Open the file and read line by line
    with open(input_file, 'r') as f:
        for line in f:
            record = json.loads(line)  # Parse each line as a JSON object

            event_name = record.get("event")
            event_date = record.get("date")

            if "group" in record and record["group"]:
                for group in record.get("group", []):
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

            else:
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
