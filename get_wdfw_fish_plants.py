import requests
import json
import time

def scrape_fish_plants(start_year=2010):
    """
    Scrapes fish plant data from the WDFW API, filtering by release_year.

    Args:
        start_year (int): The earliest year to include in the data.

    Returns:
        list: A list of dictionaries, where each dictionary is a fish plant record.
    """
    # The API endpoint for the WDFW-Fish Plants dataset
    API_URL = "https://data.wa.gov/resource/6fex-3r7d.json"

    # Socrata API query parameters
    params = {
        # Using Socrata Query Language (SoQL) to filter by year
        # `release_year >= 2010`
        "$where": f"release_year >= {start_year}",
        "$limit": 500000  # Set a high limit to get all records, Socrata's default is 1,000
    }

    try:
        print(f"Fetching fish plant data from {start_year} to present...")
        response = requests.get(API_URL, params=params, timeout=60)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        print(f"Successfully retrieved {len(data)} fish plant records.")
        return data

    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return []

def main():
    """
    Main function to orchestrate the scraping and data saving.
    """
    # Define the output file name
    OUTPUT_FILE = "wdfw_fish_plants.json"

    # Scrape the data, starting from 2010
    fish_plants = scrape_fish_plants(start_year=2010)

    if fish_plants:
        # Save the data to a JSON file
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(fish_plants, f, indent=4)
        print(f"Data saved to {OUTPUT_FILE}")
    else:
        print("No data to save. Exiting.")

if __name__ == "__main__":
    main()
