import requests
from bs4 import BeautifulSoup
import json
import re
from urllib.parse import urljoin, urlparse
import time

# Best practice to set a user-agent to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_all_paths(base_url, path_pattern):
    """
    Crawls a website starting from a base URL and finds all links matching a given pattern.
    
    Args:
        base_url (str): The starting URL for the crawl.
        path_pattern (str): A regex pattern to match desired paths.
    
    Returns:
        list: A list of unique, absolute URLs that match the pattern.
    """
    to_visit = [base_url]
    visited = set()
    found_paths = set()

    print(f"Starting crawl from: {base_url}")
    
    while to_visit:
        current_url = to_visit.pop(0)
        
        # Avoid revisiting pages
        if current_url in visited:
            continue
        
        visited.add(current_url)
        print(f"Visiting: {current_url}")
        
        try:
            response = requests.get(current_url, headers=HEADERS, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all anchor tags
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                
                # Resolve relative URLs to absolute URLs
                absolute_url = urljoin(base_url, href)
                
                # Check if the URL is internal and matches the desired path pattern
                if urlparse(absolute_url).netloc == urlparse(base_url).netloc and re.search(path_pattern, absolute_url):
                    found_paths.add(absolute_url)
                    
                    # If it's a new path to crawl, add it to the queue
                    if absolute_url not in visited and absolute_url not in to_visit:
                        to_visit.append(absolute_url)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {current_url}: {e}")
        
        # Be a good web citizen: pause between requests
        time.sleep(1) 
        
    return sorted(list(found_paths))

def main():
    """
    Main function to run the scraper and save the output.
    """
    BASE_URL = "https://wdfw.wa.gov/fishing/locations/high-lakes"
    PATH_PATTERN = r"https://wdfw\.wa\.gov/fishing/locations/high-lakes/.+"
    OUTPUT_FILE = "high_lakes_paths.json"
    
    # Get all the unique paths
    paths = get_all_paths(BASE_URL, PATH_PATTERN)
    
    # Save the paths to a JSON file
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(paths, f, indent=4)
    
    print(f"Found {len(paths)} unique paths and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
