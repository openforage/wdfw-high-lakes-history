import json
import re

def enrich_data():
    """
    Enriches the high lakes data with fish stocking records by matching on
    county and elevation.
    """
    LAKES_FILE = "high_lakes.json"
    PLANTS_FILE = "wdfw_fish_plants.json"
    OUTPUT_FILE = "enriched_high_lakes_data.json"

    print("Step 1: Loading data from JSON files...")
    try:
        with open(LAKES_FILE, 'r') as f:
            lakes_data = json.load(f)
        with open(PLANTS_FILE, 'r') as f:
            plants_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error: Required file not found. Please ensure both {LAKES_FILE} and {PLANTS_FILE} exist.")
        return

    print("Step 2: Standardizing and creating a lookup map from fish plants data...")
    plants_lookup = {}
    for plant in plants_data:
        try:
            # Standardize county name by stripping whitespace
            county = plant.get('county', '').strip().upper()
            
            # Standardize elevation by ensuring it's a clean numeric string
            elevation = plant.get('elevation', '')
            if elevation:
                # Assuming elevation is a string of digits, e.g., "3622"
                elevation = str(int(elevation.strip()))
            
            # If both are valid, create the key and add to lookup map
            if county and elevation:
                key = f"{county}-{elevation}"
                if key not in plants_lookup:
                    plants_lookup[key] = []
                plants_lookup[key].append(plant)
        except (ValueError, IndexError):
            # Skip records with invalid elevation values
            continue

    print("Step 3: Enriching lakes data by matching with fish plants data...")
    enriched_lakes_data = []
    
    # Track lakes that were matched to provide a summary
    matched_count = 0
    total_lakes = len(lakes_data)
    
    for lake in lakes_data:
        # Standardize county name
        lake_county = lake.get('county', '').strip().upper()

        # Extract numeric elevation from a string like "5305 feet"
        lake_elevation_str = lake.get('elevation', '')
        lake_elevation = ""
        if lake_elevation_str:
            match = re.search(r'\d+', lake_elevation_str)
            if match:
                lake_elevation = match.group(0)

        # Create the lookup key
        lookup_key = f"{lake_county}-{lake_elevation}"
        
        # Look up stocking data
        if lookup_key in plants_lookup:
            lake["plants"] = plants_lookup[lookup_key]
            matched_count += 1
        else:
            lake["plants"] = []

        enriched_lakes_data.append(lake)

    print(f"\nMatching complete: Matched {matched_count} out of {total_lakes} high lakes.")

    print("Step 4: Saving the enriched data to a new JSON file...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(enriched_lakes_data, f, indent=4)

    print(f"Successfully created {OUTPUT_FILE}")

if __name__ == "__main__":
    enrich_data()