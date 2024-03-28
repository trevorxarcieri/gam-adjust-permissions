import csv
import datetime
import sys
import subprocess
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import threading

if len(sys.argv) < 2:
    sys.argv.append('all')

print('Running... (press ctrl+c to interrupt)')
# Open a log file in append mode
log_file = open(f'{sys.argv[0]}.log', 'a')
# Redirect stdout to the log file
sys.stdout = log_file
print('Log file opened.')

# Initialize an Event to signal the threads to stop
stop_event = threading.Event()

# Initialize a lock for thread-safe writing to results list
results_lock = threading.Lock()

# Initial empty list to store results; make it accessible for the interrupt handler
header = []
results = []

# Get the user from the command line
user = sys.argv[1]

def keyboardInterruptHandler(signal, frame):
    print("\nKeyboardInterrupt (ID: {}) has been caught. Cleaning up...".format(signal))
    stop_event.set()  # Signal all threads to stop
    
    # Wait a bit for threads to finish current work
    threading.Timer(1.0, finalize_results).start()

# Global flag to track if finalize_results has been called
finalize_called = False

def finalize_results():
    global header, results, user, log_file, finalize_called
    if finalize_called:
        return  # Prevent double execution
    finalize_called = True
    header += ['permId']
    mode = 'a' if os.path.exists(f'{user}_file_ids_and_permIds.csv') else 'w'
    with open(f'{user}_file_ids_and_permIds.csv', mode, newline='') as file:
        writer = csv.writer(file)
        if mode == 'w':
            writer.writerow(header)  # Write header only if file is being created
        with results_lock:
            writer.writerows(results)

    # Remember to close the log file and reset sys.stdout when you're done
    sys.stdout = sys.__stdout__
    log_file.close()
    
    print(f"\nResults saved in {user}_file_ids_and_permIds.csv.\nLog can be viewed at {sys.argv[0]}.log. Exiting now :)\n")
    sys.exit(0)

signal.signal(signal.SIGINT, keyboardInterruptHandler)

def process_row(row):
    global results
    if stop_event.is_set():  # Check if the thread should stop
        return None
    
    if len(row) < 3 or row[2] == '':
        perms = subprocess.run(['gam', 'user', row[0], 'show', 'drivefileacls', 'id', row[1]], capture_output=True, text=True).stdout
        res = re.search(r"Sigma Chi Zeta Theta.*\n\s*id: (.*)", perms)
        try:
            permId = res.groups(1)[0]
            if permId == '':
                raise Exception
            with results_lock:  # Ensure thread-safe addition to results
                results.append(row + [permId])
            return row + [permId]
        except:
            print(f"No domain-wide permission found for file: {row[1]}")
            return None
    else:
        print(f"Domain-wide permission already found for file: {row[1]}")
        return row

# Function to find the last processed file ID
def find_last_processed_file_id(user):
    try:
        with open(f'{user}_file_ids_and_permIds.csv', 'r', newline='') as file:
            last_row = None
            for last_row in csv.reader(file): pass  # Iterate to the last row
            if last_row and len(last_row) >= 2 and last_row[1] != 'id':
                return last_row[1]  # Assuming the file ID is in the second column
    except FileNotFoundError:
        pass  # File doesn't exist, which is fine on first run
    return None

# Skip rows in the input file until we find the last processed file ID
last_processed_id = find_last_processed_file_id(user)
print(f"Last processed file ID: {last_processed_id}")
start_processing = last_processed_id is None  # Start processing immediately if no last processed ID found

# Modify the reading of the input CSV file
data = []  # This will hold the rows to be processed, skipping the already processed ones
with open(f'{user}_file_ids.csv', 'r', newline='') as file:
    reader = csv.reader(file)
    header, *raw_data = [row for row in reader]
    for i in range(len(raw_data)):
        row = raw_data[i]
        if start_processing:
            data.append(row)
        elif row[1] == last_processed_id:
            print(f"Found last row processed: {row}")
            start_processing = True  # Found the last processed ID, start processing the next row

# Start processing
print("made it here")
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_row, row) for row in data]

    # Instead of collecting results immediately, allow threads to add to the results directly
    for future in as_completed(futures):
        if stop_event.is_set():  # If interrupt signal received, break loop
            print("Interrupt received, stopping...")
            break

finalize_results()  # Save results to file

