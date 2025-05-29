import os
import logging


def init_logger(verbose: bool, log_file="logs/crdb_dump.log"):
    log_level = logging.DEBUG if verbose else logging.INFO
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_file, mode='w')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logging.basicConfig(
        level=log_level,
        handlers=[file_handler, stream_handler]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging to {log_file} at level {logging.getLevelName(log_level)}")
    return logger
