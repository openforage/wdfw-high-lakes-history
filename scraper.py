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
        params = {
            'county[]': county_id,
            'page': page,
            'name': '',
            'species': ''
        }
        
        try:
            url = f"{BASE_URL}?name=&county%5B%5D={county_id}&page={page}"
            response = requests.get(url, headers=HEADERS, timeout=10, params=params)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the table containing the lake data
            table = soup.find('table')
            if not table:
                print(f"No table found on page {page}. End of results for this county.")
                break # No table means no results or end of results
            
            # Extract headers from the table to use as dictionary keys
            headers = [th.text.strip() for th in table.find('thead').find_all('th')]
            
            # Extract data from the table body
            rows = table.find('tbody').find_all('tr')
            if not rows:
                print(f"No rows found on page {page}. End of results for this county.")
                break # No more results
            
            for row in rows:
                cols = row.find_all('td')
                lake_data = {
                    'name': cols[0].find('a').text.strip(),
                    'url': f"https://wdfw.wa.gov{cols[0].find('a')['href']}",
                    'acres': cols[1].text.strip(),
                    'elevation': cols[2].text.strip(),
                    'county': cols[3].text.strip(),
                    'location_lat': cols[4].find('span', class_='latlon-lat').text.strip(),
                    'location_lon': cols[4].find('span', class_='latlon-lon').text.strip()
                }
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
    OUTPUT_FILE = "high
