"""
Objective of this code is to extract the logs of Yugabyte from Support bundle, and load it in SQLITE DB for processing and visualization.

Script execution:
1) Script is executed from outside the Support bundle dir
2) sqlitdb is installed on the same host. Script will create a file / sqlitedb with file name "logs.db" in same dir
3) Change the your_directory_path directory path in the script to provide support bundle location

Let's create a "sqlfile", which has the query to execute to generate the report for analysis 

$ cat sqlfile
select day,hour,minute,second,'LeaderStepdown' from logs where message like '%Leader stepdown request%' group by 1,2,3,4 union select day,hour,minute,second,'DNSResolveFailed' from logs where message like '%Resolve failed%' group by 1,2,3,4 order by 1,2,3,4;
$

Here, "logs.db" is the database which I hard coded in script and "logs" is the name of table where logs are uploaded as part of processing

sqlite3 logs.db ".mode csv" ".headers on" -init sqlifile > output.csv

"""

import re
import sqlite3
import os
import tarfile
import gzip
import shutil

# Location where Support bundle is located
#your_directory_path = input("Enter the root directory to search for log files: ")
your_directory_path = "/Users/kapilmaheshwari/Documents/Technical/python/logs_parsing/yb-support-bundle-omc-20240609061653.028-logs/yb-tserver-0"

# Regular expression pattern for parsing log entries
log_pattern = re.compile(r'([IWEF])(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{6}) (\d+) ([^:]+):(\d+)\] (.+)')

# Process each log file
batch_size = 1000  # Adjust the batch size as needed
data_batch = []


# extract the .gz file
def extract_gz(file_path, dest_dir):
    with gzip.open(file_path, 'rb') as f_in:
        with open(dest_dir, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


# extract the .tar.gz file
def extract_tar_gz(file_path, dest_dir):
    with tarfile.open(file_path, 'r:gz') as tar:
        tar.extractall(path=dest_dir)

# traverse the support bundle dir and unzipped .tar.gz and .gz files. 
def traverse_and_extract(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.gz') and not file.endswith('.tar.gz'):
                # Extract .gz file
                dest_file_path = os.path.splitext(file_path)[0]  # Remove .gz extension
                extract_gz(file_path, dest_file_path)
                os.remove(file_path)
                print(f"Extracted {file_path} to {dest_file_path}")
            elif file.endswith('.tar.gz'):
                # Extract .tar.gz file
                dest_dir = os.path.splitext(os.path.splitext(file_path)[0])[0]  # Remove .tar.gz extension
                extract_tar_gz(file_path, dest_dir)
                os.remove(file_path)
                print(f"Extracted {file_path} to {dest_dir}")



# Function to parse log lines and return parsed data
def parse_log_line(line, server_name):
    # Trim multiple spaces
    line = re.sub(' +', ' ', line)
    match = log_pattern.match(line)
    if match:
        log_level = match.group(1)
        month = match.group(2)
        day = match.group(3)
        hour = match.group(4)
        minute = match.group(5)
        second = match.group(6)
        microseconds = match.group(7)
        thread_id = match.group(8)
        file = match.group(9)
        line_num = match.group(10)
        message = match.group(11)
        return server_name, log_level, month, day, hour, minute, second, microseconds, thread_id, file, line_num, message
    else:
        return None

# Function to extract server name from file name. I want to load the server name along with every log line loaded in the database.
def extract_server_name(file_path):
    file_name = os.path.basename(file_path)
    parts = file_name.split('.')
    if len(parts) >= 3:
        return '.'.join(parts[:-3])
    else:
        return None

# I just want to upload / process tserver INFO logs
def list_files_with_keywords(root_dir):
    matching_files = []  # Initialize an empty list to store file paths
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if "yb-tserver" in filename and "INFO" in filename and "swp" not in filename:
                matching_files.append(os.path.join(dirpath, filename))
    return matching_files

# Connect to SQLite database
conn = sqlite3.connect('logs.db')
cursor = conn.cursor()

# Create logs table if not exists
cursor.execute('''CREATE TABLE IF NOT EXISTS logs (
                    server_name TEXT,
                    log_level TEXT,
                    month TEXT,
                    day TEXT,
                    hour TEXT,
                    minute TEXT,
                    second TEXT,
                    microseconds TEXT,
                    thread_id TEXT,
                    file TEXT,
                    line TEXT,
                    message TEXT
                );''')


traverse_and_extract(your_directory_path)

# Function to batch insert parsed data
def batch_insert(cursor, data_batch):
    cursor.executemany('''INSERT INTO logs (server_name, log_level, month, day, hour, minute, second, microseconds, thread_id, file, line, message)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''', data_batch)


file_paths = list_files_with_keywords(your_directory_path)
for log_file_path in file_paths:
    # Extract server name from the file path
    server_name = extract_server_name(log_file_path)
    if not server_name:
        print(f"Error: Log file name format is incorrect for {log_file_path}.")
        continue
    
    # Read log file and insert parsed data into SQLite database
    try:
        with open(log_file_path, 'r') as log_file:
            print (log_file_path)
            for line in log_file:
                if line.startswith(('I', 'W', 'E', 'F')):  # Check if the line starts with a log level
                    parsed_data = parse_log_line(line, server_name)
                    if parsed_data:
                        data_batch.append(parsed_data)
                        if len(data_batch) >= batch_size:
                            batch_insert(cursor, data_batch)
                            data_batch = []  # Clear the batch after inserting
    except FileNotFoundError:
        print(f"Error: File {log_file_path} not found.")

# Commit changes and close connection
conn.commit()
conn.close()

"""
sqlite3 logs.db
sqlite> select server_name,count(*) from logs group by 1 ;
yb-tserver-0.root.log|1895272
yb-tserver-1.root.log|1681379
"""
