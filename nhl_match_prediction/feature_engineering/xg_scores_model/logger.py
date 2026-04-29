import logging
from pathlib import Path


def setup_logger(name: str, log_file: Path):
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # file handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)

    # console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
