import os
import requests
from datetime import datetime
import pandas as pd

# Define the directory and file path in the GitHub repository
directory = './lightning_data'
save_path = os.path.join(directory, 'lightning_data.csv')

# Create the directory if it doesn't exist
os.makedirs(directory, exist_ok=True)

# API URL and query parameters
url = "https://api-open.data.gov.sg/v2/real-time/api/weather"
querystring = {"api": "lightning"}

# Make the request to the API
response = requests.get(url, params=querystring)

# Check if the request was successful
if response.status_code == 200:
    # Get the response data in JSON format
    data = response.json()

    # Normalize the JSON data into a flat table (Pandas DataFrame)
    df = pd.json_normalize(data)  # Flatten the JSON structure

    # Add a timestamp column for tracking
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Check if the file exists before appending
    file_exists = os.path.isfile(save_path)

    # Use pandas' append mode if the file already exists, otherwise create the file
    df.to_csv(save_path, mode='a', header=not file_exists, index=False)

    if file_exists:
        print(f"Data appended to existing file at {save_path}.")
    else:
        print(f"New dataset created and saved at {save_path}.")
else:
    print(f"Failed to retrieve data. HTTP status code: {response.status_code}")
