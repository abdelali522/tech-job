import requests
import json
import os
import time  # NEW: We need this to pause our script
from datetime import datetime
from config.settings import ADZUNA_APP_ID, ADZUNA_APP_KEY

def fetch_tech_jobs(country):
    # country = "fr"  
    
    # 1. Create an empty list to hold ALL jobs from ALL pages
    all_jobs = []
    
    # 2. We want to scrape 5 pages of data (Pages 1, 2, 3, 4, 5)
    total_pages_to_scrape = 5 
    
    print(f"Starting ingestion for {country}...")

    # 3. Start our Pagination Loop
    for page in range(1, total_pages_to_scrape + 1):
        
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"

        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "what": "data engineer",
            "results_per_page": 50, # NEW: Increased to 50 jobs per page
            "content-type": "application/json"
        }

        print(f"Fetching Page {page}...")
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            jobs_list = data.get('results', [])
            
            # Add the jobs from this page into our master list
            all_jobs.extend(jobs_list) 
            
        else:
            print(f"Failed on Page {page}. Status Code: {response.status_code}")
            break # If we hit an error, stop the loop!

        # 4. Rate Limiting: Pause for 2 seconds before asking for the next page
        time.sleep(2) 

    # --- Saving to Bronze Layer (Same as before, but outside the loop!) ---
    if all_jobs:
        bronze_dir = "storage/bronze"
        os.makedirs(bronze_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_jobs_{country}_{timestamp}.json"
        filepath = os.path.join(bronze_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(all_jobs, f, indent=4)
            
        print(f"Success! Saved a total of {len(all_jobs)} jobs to {filepath}")
    else:
        print("No jobs were collected.")

if __name__ == "__main__":
    fetch_tech_jobs("za")
    