import json
import random
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def scrape_dynamic_table(url, lake_name, county_name, max_retries=3, base_delay=1):
    """
    Renders and scrapes a dynamically loaded HTML table using Selenium with retry logic.

    Args:
        url (str): The URL of the page to scrape.
        lake_name (str): The name of the lake being scraped.
        county_name (str): The name of the county the lake is in.
        max_retries (int, optional): Maximum number of retries. Defaults to 3.
        base_delay (int, optional): Base delay in seconds for exponential backoff. Defaults to 1.

    Returns:
        list: A list of dictionaries representing the table data, or None if scraping fails after all retries.
    """
    for attempt in range(max_retries):
        options = Options()
        options.add_argument("--headless=new")
        driver = webdriver.Chrome(options=options)  # Create driver inside the loop

        try:
            formatted_lake_name = f"[{county_name}] {lake_name}"
            if len(formatted_lake_name) > 50:
                formatted_lake_name = formatted_lake_name[:24] + "..." + formatted_lake_name[-24:]
            print(f"[{formatted_lake_name:50}] Loading page in headless mode: {url}")
            driver.get(url)

            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody#dataRows tr:not(:has(div.st-loading))")))

            print(f"[{formatted_lake_name:50}] Page has rendered. Scraping data...")
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            table_caption = soup.find('caption', string='10 most recent fish plants in this lake')
            if not table_caption:
                print(f"[{formatted_lake_name:50}] Could not find the target table.")
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

            print(f"[{formatted_lake_name:50}] Successfully scraped data.")
            return data_rows

        except WebDriverException as e:
            print(f"[{formatted_lake_name:50}] WebDriverException on attempt {attempt + 1}: {e}")
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"[{formatted_lake_name:50}] Retrying in {delay:.2f}s")
            time.sleep(delay)

        except Exception as e:
            print(f"[{formatted_lake_name:50}] An unexpected error occurred on attempt {attempt + 1}: {e}")
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"[{formatted_lake_name:50}] Retrying in {delay:.2f}s")
            time.sleep(delay)

        finally:
            driver.quit()  # Ensure driver is closed after each attempt

    print(f"[{formatted_lake_name:50}] Max retries reached. Failed to scrape data.")
    return None  # Indicate failure after all retries


def fetch_lake_data(lake, max_retries=5, base_delay=1):
    """
    Fetch data for a single lake with exponential backoff.
    This function is now designed to be run in a separate process.
    """
    lake_copy = lake.copy()

    for attempt in range(max_retries):
        try:
            formatted_lake_name = f"[{lake['county']}] {lake['name']}"
            if len(formatted_lake_name) > 50:
                formatted_lake_name = formatted_lake_name[:24] + "..." + formatted_lake_name[-24:]
            scraped_data = scrape_dynamic_table(lake["url"], lake['name'], lake['county'])
            lake_copy["plants"] = scraped_data if scraped_data else []
            if scraped_data:
                print(f"[{formatted_lake_name:50}] Successfully scraped data for {lake['name']}")
            else:
                print(f"[{formatted_lake_name:50}] No data found for {lake['name']}")
            return lake_copy
        except Exception as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"[{formatted_lake_name:50}] Error fetching {lake['name']}: {e}. Retrying in {delay:.2f}s (attempt {attempt+1}/{max_retries})")
            time.sleep(delay)

    formatted_lake_name =  f"[{lake['county']}] {lake['name']}"
    if len(formatted_lake_name) > 50:
        formatted_lake_name = formatted_lake_name[:24] + "..." + formatted_lake_name[-24:]
    print(f"[{formatted_lake_name:50}] Failed to scrape data for {lake['name']} after {max_retries} attempts.")
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
    main()
    # This check is crucial for the multiprocessing library to work correctly on Windows and some
