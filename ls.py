import os
import requests
from datetime import datetime
import pandas as pd

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

            # Fixed debug output
            print("\nAPI Response Keys:")
            print(list(data.keys()))
            if 'data' in data:
                print("Data Keys:", list(data['data'].keys()))
                print("Records Count:", len(data['data'].get('records', [])))

            # Process records
            for record in data.get('data', {}).get('records', []):
                item = record.get('item', {})
                for reading in item.get('readings', []):
                    try:
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
                    except Exception as e:
                        print(f"Error processing reading: {str(e)}")
                        continue

            # Pagination control
            pagination_token = data.get('data', {}).get('paginationToken')
            if not pagination_token:
                break

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
            break
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            break
            
    return pd.DataFrame(all_strikes)

def save_data(df):
    """Save data to CSV with proper formatting"""
    if df.empty:
        print("No lightning data to save")
        return

    try:
        # Convert datetime fields
        datetime_fields = ['observation_window', 'data_updated', 'strike_time']
        for field in datetime_fields:
            df[field] = pd.to_datetime(df[field], format='ISO8601', errors='coerce')

        # File operations
        file_exists = os.path.isfile(SAVE_PATH)
        df.to_csv(SAVE_PATH, mode='a', header=not file_exists, index=False)
        print(f"Successfully saved {len(df)} new strikes to {SAVE_PATH}")

    except Exception as e:
        print(f"Failed to save data: {str(e)}")

if __name__ == "__main__":
    try:
        os.makedirs(DIRECTORY, exist_ok=True)
        lightning_df = fetch_lightning_data()
        save_data(lightning_df)
    except Exception as e:
        print(f"Critical error: {str(e)}")
        exit(1)
