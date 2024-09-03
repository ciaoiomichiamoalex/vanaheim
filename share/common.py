import json
import logging


def decode_json(json_fin: str, key: str, value: str) -> list | None:
    """Read a json file and return the objects which verify the condition {key: value}.

    :param str json_fin: Path to the json file.
    :param str key: The key name to be verified.
    :param str value: The value to look for.
    :return: A list of matching objects in the file.
    """
    with open(json_fin, 'r') as jin:
        res = list(json.load(jin))

    res = [obj for obj in res if obj[key] == value]
    return res if res else None


def logger_ini(fou: str, name: str = 'main', log_level: int | str = logging.INFO) -> logging.Logger:
    """Initialize a new logger object with custom properties.

    :param str fou: Path to the log file.
    :param str name: Name of the logger, defaults to 'main'.
    :param int or str log_level: Logging level, defaults to INFO.
    :return: The logger, created if it doesn't exist.
    """
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
