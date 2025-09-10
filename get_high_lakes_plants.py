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


def scrape_dynamic_table(url, lake_name, county_name):
    """
    Renders and scrapes a dynamically loaded HTML table using Selenium.
    Each call to this function will run in a separate, isolated process.

    Args:
        url (str): The URL of the page to scrape.
        lake_name (str): The name of the lake being scraped.
        county_name (str): The name of the county for logging purposes.

    Returns:
        list: A list of dictionaries representing the table data.
    """
    options = Options()
    options.add_argument("--headless=new")

    # Each process has its own WebDriver instance, ensuring isolation
    driver = webdriver.Chrome(options=options)

    try:
        formatted_log_name = f"[{county_name}] {lake_name}"
        if len(formatted_log_name) > 50:
            formatted_log_name = formatted_log_name[:24] + "..." + formatted_log_name[-23:]

        # Updated log message format with right padding
        print(f"[{formatted_log_name:<50}] Loading page in headless mode: {url}")
        driver.get(url)

        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody#dataRows tr:not(:has(div.st-loading))")))

        # Updated log message format
        print(f"[{formatted_log_name:<50}] Page has rendered. Scraping data...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        table_caption = soup.find('caption', string='10 most recent fish plants in this lake')
        if not table_caption:
            # Updated log message format
            print(f"[{formatted_log_name:<50}] Could not find the target table.")
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

    except IndexError:
        return []
    except WebDriverException as e:
        print(f"[{formatted_log_name:<50}] WebDriver error occurred: {e}")
        return None
    finally:
        driver.quit()


def fetch_lake_data(lake, max_retries=5, base_delay=1):
    """
    Fetches data for a single lake with exponential backoff.
    This function is designed to be run in a separate process.
    """
    lake_copy = lake.copy()

    for attempt in range(max_retries):
        scraped_data = scrape_dynamic_table(lake["url"], lake['name'], lake['county'])

        if scraped_data is not None:
            lake_copy["plants"] = scraped_data

            formatted_log_name = f"[{lake_copy['county']}] {lake_copy['name']}"
            if len(formatted_log_name) > 50:
                formatted_log_name = formatted_log_name[:24] + "..." + formatted_log_name[-23:]

            if scraped_data:
                # Updated log message format
                print(f"[{formatted_log_name:<50}] Successfully scraped stocking data ({len(scraped_data)}).")
            else:
                # Updated log message format
                print(f"[{formatted_log_name:<50}] No stocking data found.")
            return lake_copy
        else:
            formatted_log_name = f"[{lake_copy['county']}] {lake_copy['name']}"
            if len(formatted_log_name) > 50:
                formatted_log_name = formatted_log_name[:24] + "..." + formatted_log_name[-23:]

            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            # Updated log message format
            print(f"[{formatted_log_name:<50}] Error. Retrying in {delay:.2f}s (attempt {attempt+1}/{max_retries})")
            time.sleep(delay)

    # If all retries fail
    formatted_log_name = f"[{lake_copy['county']}] {lake_copy['name']}"
    if len(formatted_log_name) > 50:
        formatted_log_name = formatted_log_name[:24] + "..." + formatted_log_name[-23:]

    # Updated log message format
    print(f"[{formatted_log_name:<50}] Failed to scrape stocking data after {max_retries} attempts.")
    lake_copy["plants"] = []
    return lake_copy

def main():
    try:
        with open('high_lakes.json') as f:
            lakes = json.loads(f.read())
    except FileNotFoundError:
        print("Error: high_lakes.json not found. Please ensure the file exists.")
        return

    high_lakes_plants = []
    max_workers = 4

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_lake = {executor.submit(fetch_lake_data, lake): lake for lake in lakes}
        for future in as_completed(future_to_lake):
            lake_data = future.result()
            high_lakes_plants.append(lake_data)

    print(f"\nProcessed {len(high_lakes_plants)} lakes in total.")

    with open('high_lakes_plants.json', 'w') as f:
        json.dump(high_lakes_plants, f, indent=2)
        print("\nAll lake data saved to high_lakes_plants.json")

if __name__ == "__main__":
    main()
