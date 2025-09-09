import json
import time

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

    for lake in lakes:
        scraped_data = scrape_dynamic_table(lake["url"])
        if scraped_data:
            print("\nScraped Data:")
            print(json.dumps(scraped_data, indent=2))

            # Add the scraped data to the lake dictionary
            lake["plants"] = scraped_data
            high_lakes_plants.append(lake)

        else:
            print(f"Failed to scrape data for {lake['name']}.")
            lake["plants"] = []  # Ensure each lake has a 'plants' key, even if empty
            high_lakes_plants.append(lake)

    # After processing all lakes, save the combined data to a single JSON file
    with open('high_lakes_plants.json', 'w') as f:
        json.dump(high_lakes_plants, f, indent=2)
        print("\nAll lake data saved to high_lakes_plants.json")

if __name__ == "__main__":
    main()
