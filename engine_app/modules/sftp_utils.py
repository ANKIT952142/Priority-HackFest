import os
import pysftp
import logging
import time

# Retry logic for sftp.open
def open_with_retry(sftp, file_path, retries=3, delay=5):
    """
    Attempts to open a file on the SFTP server with retries in case of failure.
    
    :param sftp: The SFTP connection object
    :param file_path: The path to the file to open
    :param retries: The number of retry attempts
    :param delay: The delay between retries in seconds
    :return: The file object if successful, or None if all retries fail
    """
    for attempt in range(retries):
        try:
            file_obj = sftp.open(file_path)
            logging.info(f"Successfully opened file: {file_path}")
            return file_obj
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} to open file {file_path} failed: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Failed to open file {file_path} after {retries} attempts.")
                return None

# Retry logic for sftp.exists
def exists_with_retry(sftp, file_path, retries=3, delay=5):
    """
    Attempts to check if a file exists on the SFTP server with retries in case of failure.
    
    :param sftp: The SFTP connection object
    :param file_path: The path to the file to check
    :param retries: The number of retry attempts
    :param delay: The delay between retries in seconds
    :return: True if the file exists, False if all retries fail
    """
    for attempt in range(retries):
        try:
            exists = sftp.exists(file_path)
            if exists:
                logging.info(f"File {file_path} exists.")
            else:
                logging.info(f"File {file_path} does not exist.")
            return exists
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} to check existence of {file_path} failed: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Failed to check existence of {file_path} after {retries} attempts.")
                return False

# Retry logic for SFTP connection
def connect_sftp(sftp_config):
    retries = 3
    logging.info("Attempting to connect to SFTP.")

    # Disable host key checking for testing purposes (not recommended for production)
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # This disables host key checking, useful for testing or self-signed keys

    for attempt in range(retries):
        try:
            sftp = pysftp.Connection(
                host=sftp_config['SFTP_HOST'],
                username=sftp_config['SFTP_USERNAME'],
                password=sftp_config['SFTP_PASSWORD'],
                port=int(sftp_config['SFTP_PORT']),
                cnopts=cnopts  # Pass the modified cnopts to disable hostkey checking
            )
            logging.info("Successfully connected to SFTP.")
            return sftp
        except pysftp.ConnectionException as e:
            logging.error(f"Attempt {attempt + 1} failed to connect to SFTP: {e}")
        except Exception as e:
            logging.error(f"Unexpected error during attempt {attempt + 1} to connect to SFTP: {e}")
        if attempt < retries - 1:
            logging.info("Retrying in 5 seconds...")
            time.sleep(5)
        else:
            raise Exception("Failed to connect to SFTP after 3 attempts")

def get_subfolder_list(sftp, folder):
    logging.debug(f"Fetching subfolders in folder: {folder}")
    subfolders = []
    for f in sftp.listdir(folder):
        path = os.path.join(folder, f)
        try:
            if sftp.isdir(path):
                subfolders.append(f)
            else:
                logging.debug(f"{f} is not a directory")
        except Exception as e:
            logging.warning(f"Error accessing {path}: {str(e)}")
    logging.info(f"Detected subfolders: {subfolders}")
    return subfolders

def move_folder(sftp, src_folder, dest_folder, subfolder):
    logging.debug(f"Moving subfolder {subfolder} from {src_folder} to {dest_folder}")
    try:
        dest_path = os.path.join(dest_folder, subfolder)
        if not exists_with_retry(sftp, dest_path):
            logging.debug(f"Destination folder {dest_path} does not exist. Creating it.")
            sftp.mkdir(dest_path)

        logging.debug(f"Moving files from {src_folder}/{subfolder} to {dest_path}")

        for file in sftp.listdir(os.path.join(src_folder, subfolder)):
            src_file = os.path.join(src_folder, subfolder, file)
            dest_file = os.path.join(dest_path, file)
            logging.debug(f"Renaming {src_file} to {dest_file}")
            sftp.rename(src_file, dest_file)

        sftp.rmdir(os.path.join(src_folder, subfolder))
        logging.info(f"Moved subfolder {subfolder} from {src_folder} to {dest_folder}")

    except Exception as e:
        logging.error(f"Error moving subfolder {subfolder}: {e}")

def check_folder_exists(sftp, folder_name, sftp_folder):
    # This function checks if a given folder exists under the SFTP_FOLDER directory
    try:
        # Assuming this function returns a list of subfolders under SFTP_FOLDER
        subfolders = sftp.listdir(sftp_folder)
        return folder_name in subfolders
    except Exception as e:
        logging.error(f"Error checking folder {folder_name}: {e}")
        return False

def sftp_upload_folder(sftp, local_folder, remote_folder):
    """Uploads an entire local folder to the SFTP server."""
    for root, dirs, files in os.walk(local_folder):
        # Create remote directory structure
        test = os.path.relpath(root, local_folder)
        remote_path = os.path.join(remote_folder, local_folder)
        logging.info(f"remote_folder {remote_folder}")
        logging.info(f"remote_path {remote_path}")
        logging.info(f"test {test}")
        try:
            sftp.mkdir(remote_path)
        except Exception as e:
            logging.warning(f"Directory {remote_path} already exists or could not be created: {e}")

        for file in files:
            local_file_path = os.path.join(root, file)
            remote_file_path = os.path.join(remote_path, file)
            sftp.put(local_file_path, remote_file_path)
            logging.info(f"Uploaded {local_file_path} to {remote_file_path}")

def sftp_upload(sftp, local_folder, sftp_target_folder):
    """
    Uploads a local folder recursively to the specified SFTP target folder.

    :param sftp: The SFTP connection object
    :param local_folder: The path to the local folder to upload
    :param sftp_target_folder: The target folder on the SFTP server where the local folder should be uploaded
    """
    try:
        # Create the target folder on the SFTP server if it doesn't exist
        if not exists_with_retry(sftp, sftp_target_folder):
            sftp.mkdir(sftp_target_folder)
            logging.info(f"Created target folder: {sftp_target_folder}")

        # Walk through the local folder
        for root, dirs, files in os.walk(local_folder):
            # Calculate the relative path of the current folder
            relative_path = os.path.relpath(root, local_folder)
            # Create the corresponding folder on the SFTP server
            current_sftp_folder = os.path.join(sftp_target_folder, relative_path)
            if not exists_with_retry(sftp, current_sftp_folder):
                sftp.mkdir(current_sftp_folder)
                logging.info(f"Created folder on SFTP: {current_sftp_folder}")

            # Upload all files in the current folder
            for file in files:
                local_file_path = os.path.join(root, file)
                sftp_file_path = os.path.join(current_sftp_folder, file)
                logging.info(f"Uploading {local_file_path} to {sftp_file_path}")
                sftp.put(local_file_path, sftp_file_path)

        logging.info(f"Successfully uploaded folder: {local_folder} to {sftp_target_folder}")

    except Exception as e:
        logging.error(f"Error uploading folder {local_folder} to {sftp_target_folder}: {e}")
           
