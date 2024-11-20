import logging
import json
import time
from .modules.logging_config import configure_logging
from .modules.config import load_config
from .modules.redis_utils import connect_redis
from .modules.sftp_utils import connect_sftp, check_folder_exists
from flask import Flask, request, jsonify
from .modules.folder_processor import handle_subfolder, is_new_subfolder

# Configure logging
configure_logging('logs/engine.log')  # Call the function to set up logging

# Load configurations
sftp_config = load_config('config/sftp.config')
redis_config = load_config('config/redis.config')
engine_config = load_config('config/engine.config')

# Initialize Redis client
redis_client = connect_redis(redis_config)

# The application setup
app = Flask(__name__)

@app.route('/process_folder', methods=['POST'])
def process_folder():
    data = request.get_json()
    folder_name = data.get('folder_name')

    if not folder_name:
        logging.error(f"Folder name missing in query.")
        return jsonify({"error": "folder_name is required"}), 400

    if not is_new_subfolder(folder_name):
        logging.error(f"Folder name doesn't match required nomenclature.")
        return jsonify({"error": "folder_name is doesn't match required nomenclature i.e. <17 alphanumeric>-ddmmyyyyhhmmss"}), 400

    try:
        with connect_sftp(sftp_config) as sftp:
            sftp_folder = sftp_config['SFTP_FOLDER']

            # Check if the folder exists under SFTP_FOLDER
            if check_folder_exists(sftp, folder_name, sftp_folder):
                # Folder exists, call handle_subfolder
                handle_subfolder(folder_name, sftp, redis_client, sftp_config)
                
                max_retries = 2
                retry_count = 0
                  
                while retry_count < max_retries:
                    check_count_key = f"subfolder:{folder_name}:check_count"
                    if redis_client.exists(check_count_key):
                         check_count = int(redis_client.get(check_count_key))
                         if check_count < 3:
                              time.sleep(5)
                              handle_subfolder(folder_name, sftp, redis_client, sftp_config)
                              retry_count += 1
                    else:
                         logging.warning(f"Redis key {check_count_key} not found. Exiting retry loop.")
                         break
                results_path = f"{sftp_config['RESULTS_SFTP_FOLDER']}/{folder_name}/results.json"
                error_path = f"{sftp_config['RESULTS_SFTP_FOLDER']}/{folder_name}/error.json"

                if sftp.exists(results_path):
                    return jsonify({
                        "processed": "true",
                        "message": f"Folder {folder_name} underwent processing. Please refer to {results_path} for details.",
                        "location": f"{results_path}"
                    }), 200
                elif sftp.exists(error_path):
                    with sftp.open(error_path, 'r') as error_file:
                        error_content = error_file.read().decode('utf-8')
                    try:
                        error_json = json.loads(error_content)  # Parse it as a JSON object
                        return jsonify({
                            "processed": "true",
                            "error": f"An error occurred.",
                            "details": error_json  # Return the parsed JSON content directly
                        }), 500
                    except json.JSONDecodeError:
                        # If parsing fails, return it as a string (as it might not be valid JSON)
                        return jsonify({
                            "processed": "true",
                            "error": f"An error occurred. Error content: {error_content}"
                        }), 500
                else:
                    return jsonify({
                        "processed": "true",
                        "error": f"Neither results.json nor error.json found in {sftp_config['RESULTS_SFTP_FOLDER']}/{folder_name}."
                    }), 404
            else:
                return jsonify({"error": f"Folder {folder_name} not found under {sftp_folder}"}), 404
    except Exception as e:
        logging.error(f"Error processing folder {folder_name}: {e}")
        return jsonify({"error": f"Error processing folder {folder_name}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
