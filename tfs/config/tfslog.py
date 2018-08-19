import logging
import logging.config
import yaml

from db.database import Database

import pdb


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

    db = Database()
    config_path = 'config/logging.yaml'
    with open(config_path, 'rt') as f:
        config = yaml.safe_load(f.read())

    mail_settings = db.get_settings_from_db(
        ('smtp_server', 'from_addr', 'to_addr'))

    for handler in config['handlers'].keys():
        handler = config['handlers'][handler]
        class_name = handler['class']
        if 'SMTPHandler' in class_name:
            handler['mailhost'] = mail_settings['smtp_server']
            handler['fromaddr'] = mail_settings['from_addr']
            handler['toaddrs'] = mail_settings['to_addr']
    logging.config.dictConfig(config)
