import logging
from functools import wraps


def log(func):
    logger = logging.getLogger(func.__module__)
    console_handler = logging.StreamHandler()
    logger.setLevel("INFO")
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [" + func.__module__ + " .%(funcName)s] %(message)s"
    )
    file_handler = logging.FileHandler("./log/app.log", mode="a", encoding="utf-8")
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        logger.setLevel(logging.INFO)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.exception(e)

    return wrapper
