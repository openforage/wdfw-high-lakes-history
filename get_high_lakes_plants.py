import json
import random
import re
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def scrape_dynamic_table(url, lake_name, county_name, driver):
    """
    Renders and scrapes a dynamically loaded HTML table using a single Selenium driver.

    Args:
        url (str): The URL of the page to scrape.
        lake_name (str): The name of the lake being scraped.
        county_name (str): The name of the county the lake is in.
        driver: The Selenium WebDriver instance.

    Returns:
        list: A list of dictionaries representing the table data.
    """
    try:
        formatted_lake_name = f"({county_name}) {lake_name}"
        if len(formatted_lake_name) > 25:
            formatted_lake_name = formatted_lake_name[:11] + "..." + formatted_lake_name[-11:]
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

    except Exception as e:
        print(f"[{formatted_lake_name:25}] An error occurred: {e}")
        return []


def main():
    """
    Main function to orchestrate the serial scraping process.
    """
    try:
        with open('high_lakes.json') as f:
            lakes = json.loads(f.read())
    except FileNotFoundError:
        print("Error: high_lakes.json not found. Please ensure the file exists.")
        return

    all_lakes_data = []
    options = Options()
    options.add_argument("--headless=new")

    # The key change: a single driver instance for the entire session
    driver = webdriver.Chrome(options=options)

    try:
        # Loop over lakes one by one
        for lake in lakes:
            lake_copy = lake.copy()
            scraped_data = scrape_dynamic_table(lake["url"], lake['name'], lake['county'], driver)
            lake_copy["plants"] = scraped_data if scraped_data else []

            formatted_lake_name = f"({lake['county']}) {lake['name']}"
            if len(formatted_lake_name) > 25:
                formatted_lake_name = formatted_lake_name[:11] + "..." + formatted_lake_name[-11:]

            if scraped_data:
                print(f"[{formatted_lake_name:25}] Successfully scraped data.")
            else:
                print(f"[{formatted_lake_name:25}] No data found.")

            all_lakes_data.append(lake_copy)
            time.sleep(random.uniform(0.3, 0.5)) # Add a small delay between requests

    finally:
        # Ensure the driver is closed after the loop
        driver.quit()

    print(f"\nProcessed {len(all_lakes_data)} lakes in total")

    # Save the combined data to a single JSON file
    with open('all_lakes_data.json', 'w') as f:
        json.dump(all_lakes_data, f, indent=2)
    print("\nAll lake data saved to all_lakes_data.json")

if __name__ == "__main__":
    main()
