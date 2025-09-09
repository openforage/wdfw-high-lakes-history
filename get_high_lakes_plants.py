import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def scrape_dynamic_table(url):
    """
    Renders and scrapes a dynamically loaded HTML table using Selenium.

    Args:
        url (str): The URL of the page to scrape.

    Returns:
        list: A list of dictionaries representing the table data.
    """
    # Set up the WebDriver (make sure chromedriver is in your PATH)
    # Create an Options object for the Chrome browser
    options = Options()

    # Add the headless argument. Using `--headless=new` is the modern approach.
    options.add_argument("--headless=new")

    # Set up the WebDriver with the specified options
    driver = webdriver.Chrome(options=options)

    print(f"Loading page in headless mode: {url}")
    driver.get(url)

    try:
        # Wait for the table data to be loaded. We look for a <tr> that doesn't
        # have the 'st-loading' class, which indicates the content has rendered.
        # This is more reliable than a simple time.sleep().
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody#dataRows tr:not(:has(div.st-loading))")))

        print("Page has rendered. Scraping data...")

        # Get the page source after rendering and parse it with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Find the table by its caption or a unique identifier
        table_caption = soup.find('caption', text='10 most recent fish plants in this lake')
        if not table_caption:
            print("Could not find the target table.")
            return []

        table = table_caption.parent

        # Extract headers from the rendered table
        headers = [th.text.strip() for th in table.find('thead').find_all('th')]

        data_rows = []
        # Extract data rows from the tbody
        for row in table.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if not cols:
                continue

            # Create a dictionary for each row, mapping headers to cell data
            row_data = {
                headers[0]: cols[0].text.strip(),
                headers[1]: cols[1].text.strip(),
                headers[2]: cols[2].text.strip(),
                headers[3]: cols[3].text.strip(),
                headers[4]: cols[4].text.strip()
            }
            data_rows.append(row_data)

        return data_rows

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    finally:
        # It's crucial to close the browser after the script finishes
        driver.quit()

def main():
    # Example URL (replace with the actual lake page URL)
    with open('high_lakes.json') as f:
        lakes = json.loads(f.read())

    high_lakes_plants = []

    def fetch_lake_data(lake, max_retries=5, base_delay=1):
        """Fetch data for a single lake with exponential backoff"""
        lake_copy = lake.copy()  # Create a copy to avoid modifying the original

        for attempt in range(max_retries):
            try:
                scraped_data = scrape_dynamic_table(lake["url"])
                lake_copy["plants"] = scraped_data if scraped_data else []
                if scraped_data:
                    print(f"\nSuccessfully scraped data for {lake['name']}")
                else:
                    print(f"No data found for {lake['name']}")
                return lake_copy
            except Exception as e:
                # Calculate backoff with jitter
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"Error fetching {lake['name']}: {e}. Retrying in {delay:.2f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)

        # If we've exhausted all retries
        print(f"Failed to scrape data for {lake['name']} after {max_retries} attempts.")
        lake_copy["plants"] = []
        return lake_copy

    # Number of workers - adjust based on your system capabilities
    max_workers = 10

    # Process lakes in parallel with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and create a future-to-lake mapping
        future_to_lake = {executor.submit(fetch_lake_data, lake): lake for lake in lakes}

        # Process results as they complete
        for future in as_completed(future_to_lake):
            lake_data = future.result()
            high_lakes_plants.append(lake_data)

    print(f"Processed {len(high_lakes_plants)} lakes in total")

    # After processing all lakes, save the combined data to a single JSON file
    with open('high_lakes_plants.json', 'w') as f:
        json.dump(high_lakes_plants, f, indent=2)
        print("\nAll lake data saved to high_lakes_plants.json")

if __name__ == "__main__":
    main()
