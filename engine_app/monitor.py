import os
import time
import threading
import requests
import json
import logging
import stat
from modules.config import load_config
from modules.sftp_utils import connect_sftp, move_folder, check_folder_exists
from modules.logging_config import configure_logging
from modules.folder_processor import is_new_subfolder, generate_random_filename

configure_logging('logs/monitor.log')

def check_for_new_folders(sftp, monitored_folder, known_folders):
    """Check for new folders in the monitored folder."""
    try:
        # List items in the monitored folder
        current_items = sftp.listdir_attr(monitored_folder)
        # Filter out files and only include directories
        current_folders = [
            item.filename for item in current_items if stat.S_ISDIR(item.st_mode)
        ]
        new_folders = [folder for folder in current_folders if folder not in known_folders]
        return new_folders
    except Exception as e:
        logging.error(f"Error checking new folders: {e}")
        return []

def execute_requests_command(folder, engine_url):
    """Execute a POST request to process the folder and return the processed field value."""
    url = engine_url
    data = {"folder_name": folder}  # Prepare the JSON payload

    try:
        logging.info(f"Executing request to {url} with data: {data}")
        response = requests.post(url, json=data)  # Send the POST request with JSON data

        # Check if the request was successful
        response.raise_for_status()  # Raises an error for bad HTTP status codes

        # Parse the JSON response
        response_data = response.json()  # Automatically decode JSON response
        logging.info(f"Engine Response for folder {folder}: {response_data}")
        return response_data.get("processed", False)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False  # Return False in case of an error

def execute_curl_command(folder, engine_url):
    """Execute a curl command to process the folder and return the processed field value."""
    url = engine_url
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"folder_name": folder})  # Format the JSON payload

    try:
        logging.info(f"Executing curl command to {url} with data: {data}")
        # Capture the output of the curl command
        result = subprocess.run(
            [
                "curl",
                "-v",
                "-X", "POST",
                url,
                "-H", headers["Content-Type"],
                "-d", data
            ],
            check=True,
            text=True,
            capture_output=True
        )

        # Log the stdout and stderr
        logging.debug(f"Curl command stdout: {result.stdout}")
        logging.debug(f"Curl command stderr: {result.stderr}")
        
        # Parse the JSON response
        response = json.loads(result.stdout)
        logging.info(f"Engine Response for folder {folder}: {response}")
        
        # Return the value of the processed field if it exists, otherwise return False
        return response.get("processed", False)

    except Exception as e:
        logging.error(f"Error executing curl command: {e}")
        return False  # Return False in case of an error

def target_new_folder(sftp, sftp_config, folder, engine_url):
    time.sleep(2)  # Wait for 2 seconds
    max_retries = 3
    processed = False

    for attempt in range(max_retries):
        processed = execute_requests_command(folder, engine_url)
        if processed:
            logging.info(f"Successfully processed folder: {folder}")
            break  # Exit the loop if successful
        else:
            logging.warning(f"Attempt {attempt + 1} failed for folder: {folder}. Retrying...")
            if attempt < 2:
                time.sleep(10)  # Wait before retrying

    if not processed:
        with connect_sftp(sftp_config) as sftp_move:
            logging.error(f"Folder processing failed after {max_retries} attempts: {folder}. Moving to failed category")
            if check_folder_exists(sftp_move, folder, sftp_config['SFTP_FOLDER']):
                logging.error(f" Moving to failed category")  
                random_filename = generate_random_filename()
                local_results_path = os.path.join("/tmp", random_filename)
                error = {"Error": "Processing error for folder : Engine call from monitor failed"}

                with open(local_results_path, 'w') as f:
                    json.dump(error, f)
                logging.info(f"Created temporary local file with random name: {local_results_path}")

                result_path = os.path.join(sftp_config['SFTP_FOLDER'], folder, 'error.json')
                logging.info(f"result_path {result_path},{local_results_path} ")
                try:
                    sftp_move.put(local_results_path, result_path)
                except FileNotFoundError:
                    logging.warning(f"File not found during upload: {local_results_path}")
                logging.info(f"Successfully uploaded the error results as error.json for subfolder {folder}")
                os.remove(local_results_path)
                move_folder(sftp_move, sftp_config['SFTP_FOLDER'], sftp_config['FAILED_SFTP_FOLDER'], folder)

    # Safely remove the folder from known_folders

def main():
    sftp_config = load_config('config/sftp.config')
    monitor_config = load_config('config/monitor.config')

    # Connect to SFTP
    try:
        while True:           
            with connect_sftp(sftp_config) as sftp:
                logging.info(f"Scanning {sftp_config['SFTP_FOLDER']} for any new sub folders...")
                known_folders = set()
                new_folders = check_for_new_folders(sftp, sftp_config['SFTP_FOLDER'], known_folders)
                for folder in new_folders:
                    if is_new_subfolder(folder):
                        logging.info(f"New folder detected: {folder}")

                        # Create a new thread for each new folder
                        thread = threading.Thread(target=target_new_folder, args=(sftp, sftp_config, folder, monitor_config['ENGINE_URL']))
                        thread.start()  # Start the thread
                    else:
                        logging.info(f"New folder detected: {folder}. It doesn't match the defined nomenclature.")  
                        logging.info(f"Moving {folder} to rejected folder.")
                        random_filename = generate_random_filename()
                        local_results_path = os.path.join("/tmp", random_filename)
                        processing_error = {"Error": f"Folder - {folder} ineligible for engine processing as per nomenclature"}

                        with open(local_results_path, 'w') as f:
                             json.dump(processing_error, f)
                        logging.info(f"Created temporary local file with random name: {local_results_path}")

                        result_path = os.path.join(sftp_config['SFTP_FOLDER'], folder, 'error.json')
                        sftp.put(local_results_path, result_path)
                        logging.info(f"Successfully uploaded the error results as error.json for subfolder {folder}")
                        os.remove(local_results_path)
                        move_folder(sftp, sftp_config['SFTP_FOLDER'], sftp_config['REJECT_SFTP_FOLDER'], folder) 
            time.sleep(60)  # Check every 5 seconds    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
