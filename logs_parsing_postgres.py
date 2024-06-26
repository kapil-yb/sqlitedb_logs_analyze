import re
import psycopg2
import os
import tarfile
import gzip
import shutil

# Location where Support bundle is located
your_directory_path = input("Enter the root directory to search for log files: ")

# Regular expression pattern for parsing log entries
log_pattern = re.compile(r'([IWEF])(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{6}) (\d+) ([^:]+):(\d+)\] (.+)')

# Extract the .gz file
def extract_gz(file_path, dest_dir):
    with gzip.open(file_path, 'rb') as f_in:
        with open(dest_dir, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

# Extract the .tar.gz file
def extract_tar_gz(file_path, dest_dir):
    with tarfile.open(file_path, 'r:gz') as tar:
        tar.extractall(path=dest_dir)

# Traverse the support bundle dir and unzip .tar.gz and .gz files.
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
    line = re.sub(' +', ' ', line)
    match = log_pattern.match(line)
    if match:
        return (
            server_name,
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
            match.group(5),
            match.group(6),
            match.group(7),
            match.group(8),
            match.group(9),
            match.group(10),
            match.group(11)
        )
    return None

# Function to extract server name from file name
def extract_server_name(file_path):
    file_name = os.path.basename(file_path)
    parts = file_name.split('.')
    if len(parts) >= 3:
        return '.'.join(parts[:-3])
    return None

# Function to list files with specific keywords
def list_files_with_keywords(root_dir):
    matching_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if "yb-tserver" in filename and "INFO" in filename:
                matching_files.append(os.path.join(dirpath, filename))
    return matching_files

# Batch insert function
def batch_insert(cursor, batch_data):
    insert_query = '''
    INSERT INTO logs (
        server_name, log_level, month, day, hour, minute, second,
        microseconds, thread_id, file, line, message
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    cursor.executemany(insert_query, batch_data)

# Connect to PostgreSQL database
db_config = {
    'dbname': 'logs_db',
    'user': 'kapilmaheshwari',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

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

    file_paths = list_files_with_keywords(your_directory_path)
    batch_size = 1000  # Define the batch size
    batch_data = []

    for log_file_path in file_paths:
        server_name = extract_server_name(log_file_path)
        if not server_name:
            print(f"Error: Log file name format is incorrect for {log_file_path}.")
            continue

        try:
            with open(log_file_path, 'r') as log_file:
                print (log_file_path)
                for line in log_file:
                    if line.startswith(('I', 'W', 'E', 'F')):
                        parsed_data = parse_log_line(line, server_name)
                        if parsed_data:
                            batch_data.append(parsed_data)
                            if len(batch_data) >= batch_size:
                                batch_insert(cursor, batch_data)
                                conn.commit()  # Commit after each batch
                                batch_data = []  # Reset batch data
        except FileNotFoundError:
            print(f"Error: File {log_file_path} not found.")

    # Insert remaining data in the final batch
    if batch_data:
        batch_insert(cursor, batch_data)

    conn.commit()
except Exception as e:
    print(f"Error connecting to PostgreSQL database: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
