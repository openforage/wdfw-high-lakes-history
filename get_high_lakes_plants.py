import json
import random
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def scrape_dynamic_table(url, lake_name):
    """
    Renders and scrapes a dynamically loaded HTML table using Selenium.

    Args:
        url (str): The URL of the page to scrape.
        lake_name (str): The name of the lake being scraped.

    Returns:
        list: A list of dictionaries representing the table data.
    """

    options = Options()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)

    try:
        formatted_lake_name = lake_name
        if len(lake_name) > 25:
            formatted_lake_name = lake_name[:11] + "..." + lake_name[-11:]
        print(f"[{formatted_lake_name:25}] Loading page in headless mode: {url}")
        driver.get(url)

        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody#dataRows tr:not(:has(div.st-loading))")))

        print(f"[{formatted_lake_name:25}] Page has rendered. Scraping data...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        table_caption = soup.find('caption', string='10 most recent fish plants in this lake')
        if not table_caption:
            print(f"[{formatted_lake_name:25}] Could not find the target table.")
            return []

        table = table_caption.parent
        headers = [th.text.strip() for th in table.find('thead').find_all('th')]

        data_rows = []
        for row in table.find('tbody').find_all('tr'):
            cols = row.find_all('td')
            if not cols:
                continue

            row_data = {
                headers[0]: cols[0].text.strip(),
                headers[1]: cols[1].text.strip(),
                headers[2]: cols[2].text.strip(),
                headers[3]: cols[3].text.strip(),
                headers[4]: cols[4].text.strip()
            }
            data_rows.append(row_data)

        return data_rows

    # No data in the table
    except IndexError:
        return []

    finally:
        driver.quit()


def fetch_lake_data(lake, max_retries=5, base_delay=1):
    """
    Fetch data for a single lake with exponential backoff.
    This function is now designed to be run in a separate process.
    """
    lake_copy = lake.copy()

    for attempt in range(max_retries):
        try:
            formatted_lake_name = lake['name']
            if len(lake['name']) > 25:
                formatted_lake_name = lake['name'][:11] + "..." + lake['name'][-11:]
            scraped_data = scrape_dynamic_table(lake["url"], lake['name'])
            lake_copy["plants"] = scraped_data if scraped_data else []
            if scraped_data:
                print(f"[{formatted_lake_name:25}] Successfully scraped stocking data ({len(scraped_data)}).")
            else:
                print(f"[{formatted_lake_name:25}] No stocking data found.")
            return lake_copy
        except Exception as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"[{formatted_lake_name:25}] Error fetching {lake['name']}: {e}. Retrying in {delay:.2f}s (attempt {attempt+1}/{max_retries})")
            time.sleep(delay)

    formatted_lake_name = lake['name']
    if len(lake['name']) > 25:
        formatted_lake_name = lake['name'][:11] + "..." + lake['name'][-11:]
    print(f"[{formatted_lake_name:25}] Failed to scrape stocking data for {lake['name']} after {max_retries} attempts.")
    lake_copy["plants"] = []
    return lake_copy


def main():
    # Example URL (replace with the actual lake page URL)
    with open('high_lakes.json') as f:
        lakes = json.loads(f.read())

    all_lakes_data = []

    # Number of workers - typically set to the number of CPU cores
    max_workers = 4

    # Process lakes in parallel with ProcessPoolExecutor
    # This is the key change for thread safety
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and create a future-to-lake mapping
        future_to_lake = {executor.submit(fetch_lake_data, lake): lake for lake in lakes}

        # Process results as they complete
        for future in as_completed(future_to_lake):
            lake_data = future.result()
            all_lakes_data.append(lake_data)

    print(f"Processed {len(all_lakes_data)} lakes in total")

    # After processing all lakes, save the combined data to a single JSON file
    with open('all_lakes_data.json', 'w') as f:
        json.dump(all_lakes_data, f, indent=2)
        print("\nAll lake data saved to all_lakes_data.json")

if __name__ == "__main__":
    # This check is crucial for the multiprocessing library to work correctly on Windows and some other systems
    main()
