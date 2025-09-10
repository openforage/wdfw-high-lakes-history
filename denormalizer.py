import csv
import json



def denormalize_and_save_to_csv(input_json_file, output_csv_file):
    """
    Reads a JSON file with nested lists, denormalizes the data,
    and writes it to a CSV file.

    Args:
        input_json_file (str): The path to the input JSON file.
        output_csv_file (str): The path to the output CSV file.
    """
    try:
        with open(input_json_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{input_json_file}' was not found.")
        return

    denormalized_data = []

    # Iterate through each lake object
    for lake in data:
        base_info = {
            'name': lake.get('name'),
            'url': lake.get('url'),
            'acres': lake.get('acres'),
            'elevation': lake.get('elevation'),
            'county': lake.get('county'),
            'location_lat': lake.get('location_lat'),
            'location_lon': lake.get('location_lon')
        }

        # Handle the case where a lake has no plants data
        plants = lake.get('plants', [])
        if not plants:
            denormalized_data.append(base_info)
            continue

        # Iterate through each plant event for the current lake
        for plant in plants:
            row = base_info.copy()  # Start with a copy of the lake's data
            row.update(plant)       # Add the plant's data to the row
            denormalized_data.append(row)

    if not denormalized_data:
        print("No data to write. Exiting.")
        return

    # Determine the field names from the first denormalized row
    fieldnames = denormalized_data[0].keys()

    # Write the data to a CSV file
    try:
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(denormalized_data)
        print(f"Successfully denormalized {len(denormalized_data)} rows and saved to '{output_csv_file}'.")
    except IOError as e:
        print(f"An I/O error occurred while writing the CSV file: {e}")

if __name__ == "__main__":
    input_file_path = 'high_lakes_plants.json'  # Your input file
    output_file_path = 'high_lakes_plants.csv' # Your desired output file
    denormalize_and_save_to_csv(input_file_path, output_file_path)
