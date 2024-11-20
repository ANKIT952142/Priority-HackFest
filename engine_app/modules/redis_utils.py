import redis
import logging
import time

# Retry logic for Redis connection
def connect_redis(redis_config):
    retries = 3
    logging.info("Attempting to connect to Redis.")
    for attempt in range(retries):
        try:
            redis_password = redis_config.get('REDIS_PASSWORD', None)  # Default to None if no password provided
            redis_client = redis.Redis(
                host=redis_config['REDIS_HOST'],
                port=int(redis_config['REDIS_PORT']),
                db=int(redis_config['REDIS_DB']),
                password=redis_password
            )
            # Test Redis connection by pinging
            redis_client.ping()
            logging.info("Successfully connected to Redis.")
            return redis_client
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed to connect to Redis: {e}")
            if attempt < retries - 1:
                logging.info("Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise Exception("Failed to connect to Redis after 3 attempts")

def acquire_lock(folder_name, redis_client):
    logging.debug(f"Attempting to acquire lock for folder: {folder_name}")
    lock_id = redis_client.incr(folder_name)
    if lock_id == 1:
        redis_client.expire(folder_name, 1800)
        logging.info(f"Acquired lock for folder: {folder_name}")
        return True
    logging.warning(f"Lock acquisition failed for folder: {folder_name}")
    return False

def release_lock(folder_name, redis_client):
    logging.debug(f"Releasing lock for folder: {folder_name}")
    redis_client.delete(folder_name)
    logging.info(f"Released lock for folder: {folder_name}")

