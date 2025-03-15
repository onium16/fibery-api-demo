import sys
from loguru import logger

class LoggerCustom:
    def __init__(self, log_file_path, LEVEL_LOGGER):
        logger.remove()
        logger.add(
            "log/main.log",
            level="INFO",
            format="{time} :: {level} :: {message}"
        )

        # Handler for terminal output with colored formatting
        logger.add(
            log_file_path,
            format="{time} :: {level} :: {file} :: {name} :: {line} :: {message}",
            level=LEVEL_LOGGER,
            serialize=False,
            rotation="10 MB",  # Rotate log file after reaching this size
            compression="zip",  # Compress older logs
            diagnose=False      # Disable full stack traces for exceptions
        )
        logger.add(sys.stderr, level=LEVEL_LOGGER)  # Add console output with the specified log level

    def get_logger(self):
        return logger
    
if __name__ == "__main__":
    logger = LoggerCustom("log/main.log", "INFO").get_logger()
    logger.info("Hello, World!")
    logger.debug("This is a debug message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")