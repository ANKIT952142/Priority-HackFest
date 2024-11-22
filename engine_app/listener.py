from flask import Flask, request, jsonify
import random
import string
import shutil
import os
import logging
import json
from datetime import datetime
from modules.config import load_config
from modules.sftp_utils import connect_sftp, sftp_upload_folder, exists_with_retry, open_with_retry
from modules.folder_processor import is_new_subfolder
from modules.logging_config import configure_logging

app = Flask(__name__)
configure_logging('logs/listener.log')

# Load configurations
sftp_config = load_config('config/sftp.config')

def generate_random_string(length=18):
    """Generate a random alphanumeric string of fixed length."""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            logging.error(f"No file part in the request.")
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']
        if file.filename == '':
            logging.error(f"No selected file in the request.")
            return jsonify({"error": "No selected file"}), 400
        
        # Generate the random string and current timestamp
        random_string = generate_random_string()
        current_timestamp = datetime.now().strftime('%d%m%Y%H%M%S')

        # Merge the strings
        result = random_string + current_timestamp
        if is_new_subfolder(result):
            # Create a directory with the random string
            os.makedirs(result, exist_ok=True)

            # Read the uploaded JSON file
            file_content = file.read().decode('utf-8')
            data = json.loads(file_content)

            # Validate that the data is a dictionary
            #if not isinstance(data, dict):
            #    logging.error("Invalid dataset: Expected a JSON object.")
            #    return jsonify({"error": "Invalid dataset: Expected a JSON object."}), 400
            
            # Check for presence of 'rules' and 'objects'
            if 'rules' not in data or 'objects' not in data:
                logging.error("Invalid dataset: Missing 'rules' or 'objects'.")
                return jsonify({"error": "Invalid dataset: Missing 'rules' or 'objects'."}), 400

            # Extract rules and objects
            rules = data.get("rules", [])
            objects = data.get("objects", [])

            # Optionally validate that both rules and objects are lists
            if not isinstance(rules, list) or not isinstance(objects, list):
                logging.error("Invalid dataset: 'rules' and 'objects' must be lists.")
                return jsonify({"error": "Invalid dataset: 'rules' and 'objects' must be lists."}), 400

            # Save rules.json
            rules_file_path = os.path.join(result, 'rules.json')
            with open(rules_file_path, 'w') as rules_file:
                json.dump(rules, rules_file, indent=4)

            # Save objects.json
            objects_file_path = os.path.join(result, 'objects.json')
            with open(objects_file_path, 'w') as objects_file:
                json.dump(objects, objects_file, indent=4)
            
        else:
            logging.error(f"Failed to generate a unique transaction id as per nomenclature - {result}.")
            return jsonify({"error": "Failed to generate a unique transaction id as per nomenclature."}), 400
        
        try:
            with connect_sftp(sftp_config) as sftp:
                sftp_folder = sftp_config['SFTP_FOLDER']
                logging.info(f"Uploading the transaction {result} to the SFTP queue.")
                sftp_upload_folder(sftp, result, sftp_folder)
        except Exception as e:
            logging.error(f"Error in sftp upload: {e}")
            return jsonify({"Error": f"SFTP Upload Failed for transaction - {result}"}), 400
        finally:
            logging.info(f"Deleting the locl folder after SFTP upload.")
            shutil.rmtree(result)

        # Create the final message
        message = f"Transaction folder name : {result}. Please check SFTP for results after some time."
        return jsonify({"result": message})
    except Exception as e:
        logging.error(f"Error in accepting new request: {e}")
        return jsonify({"error": "An unexpected error occurred in accepting new request."}), 500  # Return a generic error response

@app.route('/check_transaction', methods=['GET'])
def check_transaction():
    transaction_id = request.args.get('transaction')
    
    if not transaction_id:
        logging.error(f"Error - Transaction ID is required for GET: {e}")
        return jsonify({"error": "Transaction ID is required."}), 400

    try:
        with connect_sftp(sftp_config) as sftp:
            results_folder=sftp_config['RESULTS_SFTP_FOLDER']
            failed_folder=sftp_config['FAILED_SFTP_FOLDER']
            rejected_folder=sftp_config['REJECT_SFTP_FOLDER']
            engine_folder=sftp_config['SFTP_FOLDER']
            # Check for results.json in folder1
            
            results_file_path = f"{results_folder}/{transaction_id}/results.json"
            if exists_with_retry(sftp, results_file_path):
                with open_with_retry(sftp, results_file_path) as results_file:
                    results_content = results_file.read().decode('utf-8')
                    logging.info(f"Found results.json for transaction - {transaction_id} - {results_file_path}")
                    return jsonify({"results": json.loads(results_content)}), 200
            
            # Check for error.json in folder2
            error_file_path = f"{failed_folder}/{transaction_id}/error.json"
            if exists_with_retry(sftp, error_file_path):
                with open_with_retry(sftp, error_file_path) as error_file:
                    error_content = error_file.read().decode('utf-8')
                    logging.info(f"Found error.json for transaction - {transaction_id} - {error_file_path}")
                    return jsonify({"error": json.loads(error_content)}), 200
            
            # Check for error.json in folder3
            error_file_path = f"{rejected_folder}/{transaction_id}/error.json"
            if exists_with_retry(sftp, error_file_path):
                with open_with_retry(sftp, error_file_path) as error_file:
                    error_content = error_file.read().decode('utf-8')
                    logging.info(f"Found error.json for transaction - {transaction_id} - {error_file_path}")
                    return jsonify({"error": json.loads(error_content)}), 200
            
            # Check if folder4 exists
            engine_path = f"{engine_folder}/{transaction_id}"
            if exists_with_retry(sftp, engine_path):
                logging.info(f"Found transaction in queue - {transaction_id} - {engine_path}")
                return jsonify({"message": "Transaction is still in processing queue. Please check after some time."}), 200
            
            # If none of the above conditions are met
            return jsonify({"message": "Couldn't find any track of the specified transaction ID."}), 404
            
    except Exception as e:
        logging.error(f"Error in checking transaction status: {e}")
        return jsonify({"error": "An unexpected error occurred while checking transaction status."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
