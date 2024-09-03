import json
import logging


def decode_json(json_in: str, *json_obj: str) -> list | None:
    with open(json_in, 'r') as jin:
        res = list(json.load(jin))

    res = [obj for obj in res if obj[json_obj[0]] == json_obj[1]]
    return res if res else None


def logger_ini(fou: str, log_level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    handler = logging.FileHandler(fou)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(name)s] %(levelname)s - %(message)s',
        '%d/%m/%Y %H:%M:%S'
    ))

    logger.addHandler(handler)
    return logger
