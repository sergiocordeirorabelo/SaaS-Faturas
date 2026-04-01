"""
Configuração centralizada de logging com saída estruturada para Railway/Docker.
"""

import logging
import sys
from src.config import settings


def setup_logger(name: str) -> logging.Logger:
    """
    Cria um logger com formatação estruturada (JSON-friendly para Railway).

    Args:
        name: Nome do logger (geralmente __name__).

    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(name)

    # Evita adicionar handlers duplicados em reloads
    if logger.handlers:
        return logger

    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# Silencia logs verbose de bibliotecas externas
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)
