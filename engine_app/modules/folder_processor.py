import os
import json
import logging
import random
import string
import re
from .redis_utils import connect_redis, acquire_lock, release_lock
from .sftp_utils import open_with_retry, exists_with_retry, move_folder
from .config import load_config
from datetime import datetime

def is_new_subfolder(folder_name):
    pattern = r'^[a-zA-Z0-9]{18}\d{14}$'
    match = re.fullmatch(pattern, folder_name)
    if not match:
        logging.debug("The Folder name doesn't match nomenclature regex.")
        return False

    # Extract the datetime part and validate it
    datetime_part = folder_name[18:]  # After the 18 characters
    try:
        datetime.strptime(datetime_part, "%d%m%Y%H%M%S")
    except ValueError as e:
        logging.debug(f"Date time validation failed in folder name: {e}")
        return False

    return True


def generate_random_filename():
    """Generate a random filename with a .json extension"""
    filename = ''.join(random.choices(string.ascii_letters + string.digits, k=10)) + '.json'
    logging.debug(f"Generated random filename: {filename}")
    return filename

def evaluate_objects(objects, rules):
    """
    Evaluate objects against rules.

    Args:
        rules (dict): JSON rules.
        objects (list): JSON objects.

    Returns:
        list: Objects satisfying all rules.
    """
    # Initialize result list
    result = []

    # Iterate over objects
    for obj in objects:
        # Assume object satisfies all rules initially
        satisfies_all_rules = True

        # Iterate over rules
        for rule in rules:
            # Extract rule key, operator, and value
            key = rule['key']
            operator = rule['operator']
            value = rule['value']

            # Evaluate rule
            if operator == 'eq':
                if obj.get(key) != value:
                    satisfies_all_rules = False
                    break
            elif operator == 'neq':
                if obj.get(key) == value:
                    satisfies_all_rules = False
                    break
            elif operator == 'gt':
                if obj.get(key) <= value:
                    satisfies_all_rules = False
                    break
            elif operator == 'lt':
                if obj.get(key) >= value:
                    satisfies_all_rules = False
                    break
            elif operator == 'gte':
                if obj.get(key) < value:
                    satisfies_all_rules = False
                    break
            elif operator == 'lte':
                if obj.get(key) > value:
                    satisfies_all_rules = False
                    break

            # String Operators
            elif operator == 'contains':  # String contains substring
                if value not in str(obj.get(key)):
                    satisfies_all_rules = False
                    break
            elif operator == 'startswith':  # String starts with substring
                if not str(obj.get(key)).startswith(value):
                    satisfies_all_rules = False
                    break
            elif operator == 'endswith':  # String ends with substring
                if not str(obj.get(key)).endswith(value):
                    satisfies_all_rules = False
                    break
            elif operator == 'matches':  # Regex pattern matching
                if not re.match(value, str(obj.get(key))):
                    satisfies_all_rules = False
                    break

            # Date/Time Operators
            elif operator == 'before':  # Date before specified value
                if obj.get(key) >= value:
                    satisfies_all_rules = False
                    break
            elif operator == 'after':  # Date after specified value
                if obj.get(key) <= value:
                    satisfies_all_rules = False
                    break
            elif operator == 'on':  # Date equals specified value
                if obj.get(key) != value:
                    satisfies_all_rules = False
                    break
            elif operator == 'between':  # Date between two values
                if obj.get(key) < value[0] or obj.get(key) > value[1]:
                    satisfies_all_rules = False
                    break

            # Array Operators
            elif operator == 'in':  # Value in list
                if value not in obj.get(key, []):
                    satisfies_all_rules = False
                    break
            elif operator == 'notin':  # Value not in list
                if value in obj.get(key, []):
                    satisfies_all_rules = False
                    break
            elif operator == 'includes':  # Value includes in array
                if value not in obj.get(key):
                    satisfies_all_rules = False
                    break
            elif operator == 'excludes':  # Value excludes from array
                if value in obj.get(key):
                    satisfies_all_rules = False
                    break

            # Logical Operators
            elif operator == 'and':  # Logical AND
                satisfied = True
                for sub_rule in value:
                    if not evaluate_objects([obj], [sub_rule]).get('result'):
                        satisfied = False
                        break
                if not satisfied:
                    satisfies_all_rules = False
                    break
            elif operator == 'or':  # Logical OR
                satisfied = False
                for sub_rule in value:
                    if evaluate_objects([obj], [sub_rule]).get('result'):
                        satisfied = True
                        break
                if not satisfied:
                    satisfies_all_rules = False
                    break
            elif operator == 'not':  # Logical NOT
                if evaluate_objects([obj], [value]).get('result'):
                    satisfies_all_rules = False
                    break

            # Null/Undefined Operators
            elif operator == 'isnull':  # Value is null
                if obj.get(key) is not None:
                    satisfies_all_rules = False
                    break
            elif operator == 'isnotnull':  # Value is not null
                if obj.get(key) is None:
                    satisfies_all_rules = False
                    break  

            #Length Operator
            elif operator == 'length':
                if len(str(obj.get(key))) != value:  # String/Array length equals specified value
                    satisfies_all_rules = False
                    break

            # Modulus
            elif operator == 'mod':  # Modulus operator
                if obj.get(key) % value != 0:
                    satisfies_all_rules = False
                    break
            
            # Division
            elif operator == 'div':  # Integer division
                if obj.get(key) // value != 0:
                    satisfies_all_rules = False
                    break                

        # Add object to result if it satisfies all rules
        if satisfies_all_rules:
            result.append(obj)

    if result:
        return {"result": result}  # Return as structured data (dictionary)
    else:
        return {"error": "No objects satisfy the rules."}

def process_subfolder(sftp, folder, subfolder, redis_client, sftp_config):
    logging.debug(f"Processing subfolder: {subfolder}")
    try:
        filenames = ['objects.json', 'rules.json']
        files = {}

        # Define the Redis key for tracking recheck count for the subfolder
        check_count_key = f"subfolder:{subfolder}:check_count"

        # Fetch the current check count from Redis (defaulting to 0 if not set)
        check_count = int(redis_client.get(check_count_key) or 0)
        check_count += 1  # Increment the check count for the subfolder

        # Update the check count in Redis
        redis_client.set(check_count_key, check_count, ex=1800)
        set_count = int(redis_client.get(check_count_key))
        logging.info(f"Current try count set for {subfolder} in redis -  {set_count}")
        # Check if files exist and insert True/False in dictionary
        for filename in filenames:
            file_path = os.path.join(folder, subfolder, filename)
            try:
                # Check file existence rather than loading file contents
                files[filename] = exists_with_retry(sftp, file_path)
            except Exception as e:
                logging.warning(f"Warning: Failed to check {filename} in subfolder {subfolder}: {str(e)}")
                files[filename] = False

        if not files['objects.json'] or not files['rules.json']:
            if check_count < 3:
                logging.warning(f"Warning: Both rules.json and objects.json not present yet for {subfolder}")
            else:
                random_filename = generate_random_filename()
                local_results_path = os.path.join("/tmp", random_filename)

                missing_files_warning = {
                    "warning": "One or more input files missing current. Might get processed after complete input later",
                    "details": {
                        "objects.json": files['objects.json'],
                        "rules.json": files['rules.json']
                    }
                }

                with open(local_results_path, 'w') as f:
                    json.dump(missing_files_warning, f)
                logging.info(f"Created temporary local file with random name: {local_results_path}")

                result_path = os.path.join(folder, subfolder, 'warning.json')
                sftp.put(local_results_path, result_path)
                logging.info(f"Successfully uploaded the warning results as warning.json for subfolder {subfolder}")
                os.remove(local_results_path)
                #move_folder(sftp, sftp_config['SFTP_FOLDER'], sftp_config['FAILED_SFTP_FOLDER'], subfolder)
                redis_client.delete(check_count_key)                
        else:
            try:
                with open_with_retry(sftp, os.path.join(folder, subfolder, 'objects.json')) as objects_file:
                    try:
                        objects_json = json.load(objects_file)
                        logging.info(f"Loaded objects.json in subfolder {subfolder}")
                        logging.info(f"Objects JSON: {objects_json}")
                    except json.JSONDecodeError as e:
                            logging.error(f"Error decoding objects.json in subfolder {subfolder}: {e}")
                            error_message = {
                                "error": "Invalid JSON syntax in file",
                                "filename": "objects.json",
                                "details": str(e)
                            }
                            local_results_path = os.path.join("/tmp", generate_random_filename())
                            with open(local_results_path, 'w') as f:
                                json.dump(error_message, f)
                            result_path = os.path.join(folder, subfolder, 'error.json')
                            sftp.put(local_results_path, result_path)
                            logging.info(f"Uploaded error.json with error for objects.json in subfolder {subfolder}")
                            os.remove(local_results_path)
                            move_folder(sftp, sftp_config['SFTP_FOLDER'], sftp_config['FAILED_SFTP_FOLDER'], subfolder)
                            redis_client.delete(check_count_key)
                            return  # Skip further processing
            except Exception as e:
                logging.error(f"Failed to open objects.json: {e}")

            try:
                with open_with_retry(sftp, os.path.join(folder, subfolder, 'rules.json')) as rules_file:
                        try:
                            rules_json = json.load(rules_file)  # Try loading JSON
                            logging.info(f"Loaded rules.json in subfolder {subfolder}")
                            logging.info(f"Rules JSON: {rules_json}")
                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding rules.json in subfolder {subfolder}: {e}")
                            error_message = {
                                "error": "Invalid JSON syntax in file",
                                "filename": "rules.json",
                                "details": str(e)
                            }
                            local_results_path = os.path.join("/tmp", generate_random_filename())
                            with open(local_results_path, 'w') as f:
                                json.dump(error_message, f)
                            result_path = os.path.join(folder, subfolder, 'error.json')
                            sftp.put(local_results_path, result_path)
                            logging.info(f"Uploaded error.json with error for rules.json in subfolder {subfolder}")
                            os.remove(local_results_path)
                            move_folder(sftp, sftp_config['SFTP_FOLDER'], sftp_config['FAILED_SFTP_FOLDER'], subfolder)
                            redis_client.delete(check_count_key)
                            return  # Skip further processing
            except Exception as e:
                logging.error(f"Failed to open objects.json: {e}")

            try:
                results = evaluate_objects(objects_json, rules_json)
                logging.info(f"Processed subfolder: {subfolder}")
                local_results_path = os.path.join("/tmp", generate_random_filename())
                with open(local_results_path, 'w') as f:
                    json.dump(results, f)
                logging.info(f"Created temporary local file for processing success: {local_results_path}")

                result_path = os.path.join(folder, subfolder, 'results.json')
                sftp.put(local_results_path, result_path)
                logging.info(f"Successfully uploaded results.json for subfolder {subfolder}")
                os.remove(local_results_path)
                move_folder(sftp, sftp_config['SFTP_FOLDER'], sftp_config['RESULTS_SFTP_FOLDER'], subfolder)
                redis_client.delete(check_count_key)
            except Exception as e:
                logging.error(f"Failed to process subfolder {subfolder}: {e}")
                random_filename = generate_random_filename()
                local_results_path = os.path.join("/tmp", random_filename)

                processing_error = {"Error": f"Processing error for folder {subfolder} - {e}"}

                with open(local_results_path, 'w') as f:
                    json.dump(processing_error, f)
                logging.info(f"Created temporary local file with random name: {local_results_path}")

                result_path = os.path.join(folder, subfolder, 'error.json')
                sftp.put(local_results_path, result_path)
                logging.info(f"Successfully uploaded the error results as error.json for subfolder {subfolder}")
                os.remove(local_results_path)
                move_folder(sftp, sftp_config['SFTP_FOLDER'], sftp_config['FAILED_SFTP_FOLDER'], subfolder)
                redis_client.delete(check_count_key)   

    except Exception as e:
        logging.error(f"Error processing subfolder {subfolder}: {e}")

def handle_subfolder(subfolder, sftp, redis_client, sftp_config):
    if is_new_subfolder(subfolder):
        if acquire_lock(subfolder, redis_client):
            try:
                process_subfolder(sftp, sftp_config['SFTP_FOLDER'], subfolder, redis_client, sftp_config)
            finally:
                release_lock(subfolder, redis_client)
        else:
            logging.warning(f"Skipping subfolder {subfolder} due to failed lock acquisition.")
    else:
        logging.info(f"Subfolder {subfolder} does not match the expected naming convention.")
