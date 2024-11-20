import json
import pandas as pd
from google.cloud import bigquery

# Function to flatten JSON and save as CSV
def flatten_and_save_to_csv(json_data, csv_file):
    flattened_data = []

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
    df.to_csv(csv_file, index=False)
    print(f"CSV file saved: {csv_file}")

# Sample JSON data
json_data = [
    {
        "event": "Event1",
        "date": "2024-11-20",
        "group": [
            {
                "group_id": "G1",
                "group_name": "Group One",
                "member": [
                    {"member_id": "M1", "name": "Alice"},
                    {"member_id": "M2", "name": "Bob"}
                ]
            },
            {
                "group_id": "G2",
                "group_name": "Group Two",
                "member": [
                    {"member_id": "M3", "name": "Charlie"}
                ]
            }
        ]
    },
    {
        "event": "Event2",
        "date": "2024-11-21",
        "group": [
            {
                "group_id": "G3",
                "group_name": "Group Three",
                "member": [
                    {"member_id": "M4", "name": "David"}
                ]
            }
        ]
    }
]

# Flatten JSON and save as CSV
flatten_and_save_to_csv(json_data, "flattened_data.csv")
