import json
import time
from datetime import datetime

import requests


def fetch_and_save():
    print(f"[{datetime.now()}] Starting fetch of WDFW fish plants data...")

    base_url = "https://data.wa.gov/resource/6fex-3r7d.json"
    all_data = []
    offset = 0
    limit = 1000

    while True:
        url = f"{base_url}?$limit={limit}&$offset={offset}"
        try:
            print(f"[{datetime.now()}] Fetching data from offset {offset}...")
            response = requests.get(url)
            response.raise_for_status()

            page_data = response.json()
            all_data.extend(page_data)

            # If we received fewer records than the limit, we've reached the end
            if len(page_data) < limit:
                break

            # Increment offset for the next page
            offset += limit

            # Be kind to the API - small delay between requests
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"[{datetime.now()}] Error during scrape: {e}")
            break

    # Prepare the results
    result = {
        "source": "WA State Data",
        "last_updated": str(datetime.now()),
        "status": "success",
        "message": f"Data scraped and processed. Total records: {len(all_data)}",
        "data": all_data
    }

    # Save the data to a shared volume
    with open("wdfw_fish_plants.json", "w") as f:
        json.dump(result, f, indent=2)

    print(f"[{datetime.now()}] Data scrape finished. {len(all_data)} records saved to wdfw_fish_plants.json")

if __name__ == "__main__":
    fetch_and_save()
