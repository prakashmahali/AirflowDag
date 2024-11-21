import json
import pandas as pd

# Recursive function to flatten a nested JSON structure
def flatten_json(nested_json, parent_key='', sep='_'):
    items = {}
    
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep=sep))
        elif isinstance(value, list):
            for i, sub_value in enumerate(value):
                items.update(flatten_json(sub_value, f"{new_key}_{i}", sep=sep))
        else:
            items[new_key] = value
    
    return items

# Sample nested JSON data (with 3 levels)
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
    },
    {
        "event": "Event3",
        "date": "2024-11-22",
        "group": [
            {
                "group_id": "G4",
                "group_name": "Group Four",
                "member": []
            }
        ]
    },
    {
        "event": "Event4",
        "date": "2024-11-23",
        "group": [
            {
                "group_id": "G5",
                "group_name": "Group Five",
                "member": None  # Empty member list
            }
        ]
    }
]

# Flatten each JSON record and store the result
flattened_data = []
for record in json_data:
    flattened_data.append(flatten_json(record))

# Convert the flattened data into a pandas DataFrame
df = pd.DataFrame(flattened_data)

# Save to CSV (or use other methods to store, like saving to BigQuery)
df.to_csv('flattened_data.csv', index=False)

print(df)
