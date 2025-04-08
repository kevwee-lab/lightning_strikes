import os
import requests
from datetime import datetime
import pandas as pd
import json

# Configuration
DIRECTORY = '.'  # Root directory
SAVE_PATH = os.path.join(DIRECTORY, 'lightning_data.csv')
API_URL = "https://api-open.data.gov.sg/v2/real-time/api/weather"

def fetch_lightning_data():
    """Fetch paginated lightning data from API"""
    params = {"api": "lightning", "date": datetime.now().strftime('%Y-%m-%d')}
    pagination_token = None
    all_strikes = []
    
    while True:
        try:
            if pagination_token:
                params["paginationToken"] = pagination_token
            else:
                params.pop("paginationToken", None)

            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') != 0:
                print(f"API Error: {data.get('errorMsg', 'Unknown error')}")
                break

            # Debugging output
            print("\nAPI Response Structure:")
            print(json.dumps({k: type(v) for k, v in data.items()}, indent=2))
            
            # Process records
            for record in data.get('data', {}).get('records', []):
                item = record.get('item', {})
                for reading in item.get('readings', []):
                    strike = {
                        'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'observation_window': record.get('datetime'),
                        'data_updated': record.get('updatedTimestamp'),
                        'latitude': float(reading.get('location', {}).get('latitude', 0)),
                        'longitude': float(reading.get('location', {}).get('longitude', 0)),
                        'strike_type': reading.get('type'),
                        'description': reading.get('text'),
                        'strike_time': reading.get('datetime')
                    }
                    all_strikes.append(strike)

            # Pagination control
            pagination_token = data.get('data', {}).get('paginationToken')
            if not pagination_token:
                break

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
            break
            
    return pd.DataFrame(all_strikes)

def save_data(df):
    """Save data to CSV with proper formatting"""
    if df.empty:
        print("No lightning data to save")
        return

    # Convert datetime fields
    datetime_fields = ['observation_window', 'data_updated', 'strike_time']
    for field in datetime_fields:
        df[field] = pd.to_datetime(df[field], format='ISO8601')

    # File operations
    file_exists = os.path.isfile(SAVE_PATH)
    df.to_csv(SAVE_PATH, mode='a', header=not file_exists, index=False)
    print(f"Saved {len(df)} new strikes to {SAVE_PATH} (appended: {file_exists})")

if __name__ == "__main__":
    os.makedirs(DIRECTORY, exist_ok=True)
    lightning_df = fetch_lightning_data()
    save_data(lightning_df)
