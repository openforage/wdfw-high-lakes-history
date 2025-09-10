name: Denormalize Data

# This workflow is triggered manually and on a schedule.
on:
  workflow_dispatch:
  schedule:
    # Run once a day at 1:00 AM UTC
    - cron: '0 1 * * *'

# Defines a single job for denormalizing and committing the data.
jobs:
  denormalize-and-commit:
    runs-on: ubuntu-latest
    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.x'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run denormalizer
        run: python denormalizer.py

      # Step 5: Commit and push the new or updated JSON file.
      # This action automatically commits the changes if the output file is different.
      - name: Commit and push changes
        uses: EndBug/add-and-commit@v9
        with:
          author_name: Git Scraping Bot
          author_email: actions@github.com
          message: 'Denormalize high lakes fish plant data'
          # Add the specific output file to be committed.
          add: 'high_lakes_plants_flattened.json'
          push: true
