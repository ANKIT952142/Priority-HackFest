import logging

def configure_logging(log_file):
    # Enable paramiko logging for better debugging
    paramiko_logger = logging.getLogger("paramiko")
    paramiko_logger.setLevel(logging.DEBUG)

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Log to stdout (console)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Set to DEBUG if you want more detailed logs on console
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logging.getLogger().addHandler(console_handler)

