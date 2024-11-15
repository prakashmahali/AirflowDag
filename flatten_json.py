import json
import pandas as pd

def flatten_json(json_file, output_csv):
    # Load the JSON data from the file
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Ensure 'group' field is a list, even if it's missing or empty
    groups = data.get("group", [])
    if not groups:
        groups = [{}]  # If 'group' is empty, create a placeholder

    flattened_data = []

    # Iterate through each group
    for group in groups:
        group_name = group.get("group_name", None)
        group_id = group.get("group_id", None)
        members = group.get("member", [])

        # If 'member' is missing or empty, create a placeholder
        if not members:
            members = [{}]

        # Iterate through each member
        for member in members:
            member_id = member.get("member_id", None)

            # Flattened record for each member
            flattened_record = {
                "event_id": data.get("event_id", None),
                "event_name": data.get("event_name", None),
                "group_name": group_name,
                "group_id": group_id,
                "member_id": member_id
            }

            # Add to the list of flattened records
            flattened_data.append(flattened_record)

    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)

    # Save to CSV file
    df.to_csv(output_csv, index=False)
    print(f"Flattened data saved to {output_csv}")

# Example usage
flatten_json('nested_data.json', 'flattened_data.csv')
