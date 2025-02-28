import psycopg2
import csv
from datetime import datetime, timedelta
import os  # To extract filename from path

# PostgreSQL Connection Details
db_params = {
    "dbname": "verve",
    "user": "postgres",
    "password": "Avis!123",
    "host": "192.168.160.229",
    "port": "5433"  # Your PostgreSQL is running on port 5433
}

# Get Current Date & Time in Required Format
current_date = datetime.now().strftime("%Y%m%d")  # YYYYMMDD format
current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # YYYYMMDD_HHMMSS format

# Generate Dynamic CSV File Name
csv_file = f"bharatpe_{current_timestamp}.csv"

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# SQL Query to fetch only today's records
query = f"""
SELECT 
    '{current_date}' AS ftpPath,  -- Only current date folder name
    r.recfilename AS fileName,  -- Avoiding extra path
    r.accountcode AS key1,
    'COGENT' AS vendor,  
    c.calltype AS callType,
    c.callduration AS callDuration,
    c.phonenumber AS ANI,
    c.callstartdate AS CREATED,
    u.name AS agentName
FROM cr_recording_log r
JOIN cr_conn_cdr c 
    ON r.accountcode = c.accountcode 
    AND c.callstartdate::DATE = CURRENT_DATE  
LEFT JOIN ct_user u 
    ON c.agentid = u.id
WHERE r.eventdate::DATE = CURRENT_DATE;
"""

cursor.execute(query)
records = cursor.fetchall()

# Writing Data to CSV
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    
    # Writing Header Row (AgentId removed)
    writer.writerow([
        "ftpPath", "fileName", "key1", "vendor", "callType", "callDuration",
        "ANI", "CREATED", "agentid", "fileSize"  # Removed AgentId
    ])
    
    # Writing Data Rows
    for row in records:
        ftpPath, fileName, key1, vendor, callType, callDuration, ANI, CREATED, agentName = row
        
        # Extract only filename (remove any folder path)
        fileName = os.path.basename(fileName)

        # Convert callType: "OUT" → "OUTBOUND", "IN" → "INBOUND"
        if callType == "OUT":
            callType = "OUTBOUND"
        elif callType == "IN":
            callType = "INBOUND"

        # Convert callDuration to HH:MM:SS format
        if callDuration is not None:
            seconds = int(callDuration)
            callDuration = str(timedelta(seconds=seconds))  # Converts to HH:MM:SS format
        else:
            callDuration = "00:00:00"  # Default if null
        
        # Add an empty fileSize column
        fileSize = ""

        # Write formatted row (AgentId removed)
        writer.writerow([
            ftpPath, fileName, key1, vendor, callType, callDuration,
            ANI, CREATED, agentName, fileSize
        ])

# Close connection
cursor.close()
conn.close()

print(f"CSV file '{csv_file}' has been created successfully with formatted data!")
