import requests
from bs4 import BeautifulSoup
import json
import time

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
        print(f"Fetching county IDs from: {url}")
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes

        soup = BeautifulSoup(response.text, 'html.parser')
        county_select_tag = soup.find('select', {'name': 'county[]'})

        if not county_select_tag:
            print("Error: Could not find the county select element.")
            return []

        county_ids = [option.get('value') for option in county_select_tag.find_all('option') if option.get('value')]
        
        return county_ids

    except requests.exceptions.RequestException as e:
        print(f"Error during HTTP request for county IDs: {e}")
        return []

def scrape_lakes_per_county(county_id):
    """
    Scrapes all high lakes for a given county ID, handling pagination.
    
    Args:
        county_id (str): The ID of the county to scrape.
    
    Returns:
        list: A list of dictionaries, where each dictionary represents a lake.
    """
    print(f"\n--- Scraping lakes for county ID: {county_id} ---")
    
    BASE_URL = "https://wdfw.wa.gov/fishing/locations/high-lakes"
    all_lakes_for_county = []
    page = 0
    while True:
        # Construct the URL with query parameters
        url = f"{BASE_URL}?name=&county%5B%5D={county_id}&page={page}"
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the table containing the lake data
            table = soup.find('table')
            if not table:
                print(f"No table found on page {page}. End of results for this county.")
                break 
            
            rows = table.find('tbody').find_all('tr')
            if not rows:
                print(f"No rows found on page {page}. All pages scraped for this county.")
                break 
            
            for row in rows:
                cols = row.find_all('td')
                
                # Use a dictionary to store lake data
                lake_data = {}

                # Name and URL (these are always present)
                name_tag = cols[0].find('a')
                if name_tag:
                    lake_data['name'] = name_tag.text.strip()
                    lake_data['url'] = f"https://wdfw.wa.gov{name_tag['href']}"
                else:
                    lake_data['name'] = ""
                    lake_data['url'] = ""

                # Acres: Handle potential missing data
                try:
                    lake_data['acres'] = cols[1].text.strip()
                except IndexError:
                    lake_data['acres'] = ""
                    
                # Elevation: Handle potential missing data
                try:
                    lake_data['elevation'] = cols[2].text.strip()
                except IndexError:
                    lake_data['elevation'] = ""

                # County: Always present as it's the basis for the query
                lake_data['county'] = cols[3].text.strip()

                # Location: Handle potential missing data and sub-elements
                try:
                    location_tag = cols[4]
                    lat_tag = location_tag.find('span', class_='latlon-lat')
                    lon_tag = location_tag.find('span', class_='latlon-lon')
                    
                    lake_data['location_lat'] = lat_tag.text.strip() if lat_tag else ""
                    lake_data['location_lon'] = lon_tag.text.strip() if lon_tag else ""
                except IndexError:
                    lake_data['location_lat'] = ""
                    lake_data['location_lon'] = ""
                
                all_lakes_for_county.append(lake_data)
                
            print(f"Scraped {len(rows)} lakes from page {page}")
            
            # Check for a 'next' link to determine if there are more pages
            next_link = soup.find('li', class_='pager__item--next')
            if not next_link:
                print("No 'Next' link found. All pages scraped.")
                break
                
            page += 1
            time.sleep(1) # Be a good web citizen

        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page} for county ID {county_id}: {e}")
            break
            
    return all_lakes_for_county

def main():
    """
    Main function to orchestrate the scraping and data saving.
    """
    BASE_URL = "https://wdfw.wa.gov/fishing/locations/high-lakes"
    OUTPUT_FILE = "high_lakes.json"

    # Step 1: Get the list of county IDs
    county_ids = get_county_ids(BASE_URL)
    if not county_ids:
        print("No county IDs found. Exiting script.")
        return

    all_high_lakes = []
    # Step 2: Loop through each county ID and scrape all lakes
    for county_id in county_ids:
        lakes = scrape_lakes_per_county(county_id)
        all_high_lakes.extend(lakes)

    # Save the consolidated data to a single JSON file
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(all_high_lakes, f, indent=4)
    
    print(f"\nSuccessfully scraped {len(all_high_lakes)} lakes across all counties and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
