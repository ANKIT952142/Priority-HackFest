import logging

def load_config(filename):
    logging.debug(f"Loading configuration from {filename}")
    config = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                key, value = line.strip().split('=')
                config[key] = value
        logging.info(f"Loaded configuration from {filename}")
    except Exception as e:
        logging.error(f"Failed to load configuration from {filename}: {e}")
        raise
    return config
