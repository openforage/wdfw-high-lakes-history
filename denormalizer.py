import json

def denormalize_and_save_to_json(input_json_file, output_json_file):
    """
    Reads a nested JSON file, denormalizes the data, and
    writes it to a new JSON file with a flattened structure.

    Args:
        input_json_file (str): The path to the input JSON file.
        output_json_file (str): The path to the output JSON file.
    """
    try:
        with open(input_json_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{input_json_file}' was not found.")
        return

    denormalized_data = []
    
    # Iterate through each lake object in the main list
    for lake in data:
        # Define the base information for each lake
        base_info = {
            'name': lake.get('name'),
            'url': lake.get('url'),
            'acres': lake.get('acres'),
            'elevation': lake.get('elevation'),
            'county': lake.get('county'),
            'location_lat': lake.get('location_lat'),
            'location_lon': lake.get('location_lon')
        }

        # Handle lakes with or without plant data
        plants = lake.get('plants', [])
        if not plants:
            # If no plant data, create one entry with lake info and null plant fields
            row = base_info.copy()
            row.update({
                'stock_date': None,
                'species': None,
                'number_released': None,
                'number_of_fish_per_pound': None,
                'facility': None
            })
            denormalized_data.append(row)
            continue
        
        # Iterate through each plant event and combine with lake data
        for plant in plants:
            row = base_info.copy()
            # Update the row with the plant's data, which is already a dictionary
            row.update(plant)
            denormalized_data.append(row)

    if not denormalized_data:
        print("No data to write. Exiting.")
        return

    # Write the denormalized data to the specified output JSON file
    try:
        with open(output_json_file, 'w') as f:
            json.dump(denormalized_data, f, indent=2)
        print(f"Successfully denormalized {len(denormalized_data)} records and saved to '{output_json_file}'.")
    except IOError as e:
        print(f"An I/O error occurred while writing the JSON file: {e}")

if __name__ == "__main__":
    input_file_path = 'high_lakes_plants.json'
    output_file_path = 'high_lakes_plants_flattened.json'
    denormalize_and_save_to_json(input_file_path, output_file_path)
