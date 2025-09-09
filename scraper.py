import requests
from bs4 import BeautifulSoup
import json

# Best practice to set a user-agent to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_county_ids(url):
    """
    Fetches the HTML from a URL and extracts a list of county IDs.

    Args:
        url (str): The URL of the page to scrape.

    Returns:
        list: A list of unique county IDs as strings.
    """
    try:
        print(f"Fetching HTML from: {url}")
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the specific <select> element for counties
        # The key is to use a unique identifier, like the 'name' attribute.
        county_select_tag = soup.find('select', {'name': 'county[]'})

        if not county_select_tag:
            print("Error: Could not find the county select element.")
            return []

        county_ids = []
        # Iterate through all <option> tags within the found <select> tag
        for option in county_select_tag.find_all('option'):
            county_id = option.get('value')
            if county_id:
                county_ids.append(county_id)
        
        return county_ids

    except requests.exceptions.RequestException as e:
        print(f"Error during HTTP request: {e}")
        return []

def main():
    """
    Main function to orchestrate the scraping and data saving.
    """
    BASE_URL = "https://wdfw.wa.gov/fishing/locations/high-lakes"
    OUTPUT_FILE = "county_ids.json"

    # Get the county IDs
    county_ids = get_county_ids(BASE_URL)

    if not county_ids:
        print("No county IDs found. Exiting.")
        return

    # Print the found IDs for verification
    print(f"Found {len(county_ids)} county IDs:")
    print(county_ids)
    
    # Save the IDs to a JSON file
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(county_ids, f, indent=4)
    
    print(f"Successfully scraped and saved county IDs to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
