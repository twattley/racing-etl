import logging


def get_logging_config(log_type):
    """
    Logging configuration for balance module

    """
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-2s - %(message)s",
        level=log_type,
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    return logging.getLogger(__name__)


def print_information(msg):
    logger = get_logging_config(logging.INFO)
    logger.info(msg)


def print_warning(msg):
    logger = get_logging_config(logging.WARNING)
    logger.warning(msg)


def print_error(msg):
    logger = get_logging_config(logging.ERROR)
    logger.error(msg)


def print_debug(msg):
    logger = get_logging_config(logging.DEBUG)
    logger.debug(msg)


def print_critical(msg):
    logger = get_logging_config(logging.CRITICAL)
    logger.critical(msg)


I = print_information  # noqa
W = print_warning  # noqa
E = print_error  # noqa
D = print_debug  # noqa
C = print_critical  # noqa
