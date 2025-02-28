import requests
import csv
import os
import glob

# Define API Endpoint
API_URL = "https://your-api-endpoint.com/v3/ivr/file-upload-callback"  # Replace with actual API URL

# Define Headers (Modify Authorization if required)
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Basic your_auth_key"  # Replace with actual authorization key
}

# Folder where CSV files are stored
folder_path = "/var/jenkins_home/workspace/bharatpe/"

# Find the latest CSV file in the folder
csv_files = glob.glob(os.path.join(folder_path, "bharatpe_*.csv"))  # Find all matching files

if not csv_files:
    print("Error: No CSV files found in the folder!")
    exit(1)

latest_csv = max(csv_files, key=os.path.getctime)  # Get the most recently created file
print(f"Latest file found: {latest_csv}")

# Read CSV and Push Data to API
with open(latest_csv, mode="r", newline="") as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        # Prepare API Payload
        payload = {
            "ftpPath": row["ftpPath"],
            "fileName": row["fileName"],
            "key1": row["key1"],
            "vendor": row["vendor"],
            "callType": row["callType"],
            "fileSize": row["fileSize"],  
            "callDuration": row["callDuration"],
            "DNIS": row["DNIS"],  # Mapping DNIS from ANI column if needed
            "ANI": row["ANI"],
            "CREATED": row["CREATED"],
            "agentName": row["agentName"],
            "AgentId": row.get("agentid", "")  # Set empty if not available
        }

        # Send API Request
        response = requests.post(API_URL, json=payload, headers=HEADERS)

        # Print API Response
        print(f"Sent: {payload}")
        print(f"Response ({response.status_code}): {response.text}\n")

print("All records from the latest file have been pushed successfully!")
