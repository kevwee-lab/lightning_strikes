import os
import requests
from datetime import datetime, timedelta
import pandas as pd

# Configuration
DIRECTORY = 'lightning_data'
API_URL = "https://api-open.data.gov.sg/v2/real-time/api/weather"

def fetch_lightning_data():
    """Fetch yesterday's lightning data (Singapore date)"""
    # Calculate yesterday in UTC+8 (Singapore) time
    utc_now = datetime.utcnow()
    yesterday_sg = (utc_now + timedelta(hours=8)) - timedelta(days=1)
    
    params = {
        "api": "lightning",
        "date": yesterday_sg.strftime('%Y-%m-%d')  # Previous day's date
    }
    pagination_token = None
    all_strikes = []
    
    while True:
        try:
            if pagination_token:
                params["paginationToken"] = pagination_token
            else:
                params.pop("paginationToken", None)

            response = requests.get(API_URL, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('code') != 0:
                print(f"API Error: {data.get('errorMsg', 'Unknown error')}")
                break

            # Process records (no datetime conversion needed)
            for record in data.get('data', {}).get('records', []):
                item = record.get('item', {})
                for reading in item.get('readings', []):
                    try:
                        location = reading.get('location', {})
                        strike = {
                            'fetch_time': (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
                            'observation_window': record.get('datetime'),
                            'data_updated': record.get('updatedTimestamp'),
                            'latitude': float(location.get('latitude', 0)),
                            'longitude': float(location.get('longitude', 0)),
                            'strike_type': reading.get('type'),
                            'description': reading.get('text'),
                            'strike_time': reading.get('datetime')
                        }
                        all_strikes.append(strike)
                    except Exception as e:
                        print(f"Error processing reading: {str(e)}")
                        continue

            pagination_token = data.get('data', {}).get('paginationToken')
            if not pagination_token:
                break

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
            break
            
    return pd.DataFrame(all_strikes)

def save_data(df):
    if df.empty:
        print("No data to save")
        return

    # Use same date as API call
    utc_now = datetime.utcnow()
    yesterday_sg = (utc_now + timedelta(hours=8)) - timedelta(days=1)
    filename = f"lightning_{yesterday_sg.strftime('%Y-%m-%d')}.csv"
    SAVE_PATH = os.path.join(DIRECTORY, filename)
    
    # Save raw ISO8601 strings (already in +08:00 timezone)
    df.to_csv(SAVE_PATH, index=False)
    print(f"Saved {len(df)} strikes to {SAVE_PATH}")

if __name__ == "__main__":
    try:
        lightning_df = fetch_lightning_data()
        save_data(lightning_df)
    except Exception as e:
        print(f"Critical error: {str(e)}")
        exit(1)
