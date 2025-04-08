import os
import requests
from datetime import datetime
import pandas as pd
import json

# Define the directory and file path in the GitHub repository
directory = '.'  # Changed to root directory
save_path = os.path.join(directory, 'lightning_data.csv')

# API URL and query parameters
url = "https://api-open.data.gov.sg/v2/real-time/api/weather"
querystring = {"api": "lightning"}

# Make the request to the API
response = requests.get(url, params=querystring)

# Check if the request was successful
if response.status_code == 200:
    # Get the response data in JSON format
    data = response.json()
    
    # Print the full response for debugging
    print("API Response:")
    print(json.dumps(data, indent=2))
    
    # Extract relevant information
    # Check if 'items' exists in the response
    if 'items' in data:
        items = data['items']
        
        # Prepare data for CSV
        rows = []
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for item in items:
            # Extract datetime if available
            item_datetime = item.get('datetime', 'unknown')
            
            # Extract readings if available
            if 'item' in item and 'readings' in item['item']:
                readings = item['item']['readings']
                
                if readings:
                    for reading in readings:
                        row = {
                            'timestamp': timestamp,
                            'datetime': item_datetime,
                            'location': reading.get('station_id', 'unknown'),
                            'latitude': reading.get('lat', 'unknown'),
                            'longitude': reading.get('lng', 'unknown'),
                            'value': reading.get('value', 'unknown')
                        }
                        rows.append(row)
                else:
                    # If no readings, still record the event
                    row = {
                        'timestamp': timestamp,
                        'datetime': item_datetime,
                        'location': 'no_reading',
                        'latitude': 'unknown',
                        'longitude': 'unknown',
                        'value': 'no_data'
                    }
                    rows.append(row)
            else:
                # Just record the API call if no proper structure
                row = {
                    'timestamp': timestamp,
                    'datetime': item_datetime,
                    'location': 'api_call',
                    'latitude': 'unknown',
                    'longitude': 'unknown',
                    'value': 'no_structure'
                }
                rows.append(row)
                
        # Convert to DataFrame
        if rows:
            df = pd.DataFrame(rows)
            
            # Check if the file exists before appending
            file_exists = os.path.isfile(save_path)
            
            # Use pandas' append mode if the file already exists, otherwise create the file
            df.to_csv(save_path, mode='a', header=not file_exists, index=False)
            
            print(f"Data processed: {len(rows)} records")
            if file_exists:
                print(f"Data appended to existing file at {save_path}.")
            else:
                print(f"New dataset created and saved at {save_path}.")
        else:
            print("No data to save.")
    else:
        print("No 'items' found in the API response.")
        print("Full response:", data)
else:
    print(f"Failed to retrieve data. HTTP status code: {response.status_code}")
