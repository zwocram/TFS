import logging
import logging.config
import yaml


def setup_logging():
    """Setup logging information.

    Level       Numeric Value
    =========================
    CRITICAL    50
    ERROR       40
    WARNING     30
    INFO        20
    DEBUG       10

    """
    config_path = 'config/logging.yaml'
    with open(config_path, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
