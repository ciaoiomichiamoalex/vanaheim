import json
import logging


def decode_json(json_fin: str, key: str, value: str) -> list | None:
    with open(json_fin, 'r') as jin:
        res = list(json.load(jin))

    res = [obj for obj in res if obj[key] == value]
    return res if res else None


def logger_ini(fou: str, name: str = 'main', log_level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not len(logger.handlers):
        logger.setLevel(log_level)

        handler = logging.FileHandler(fou)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s - %(message)s',
            '%d/%m/%Y %H:%M:%S'
        ))

        logger.addHandler(handler)
    return logger
