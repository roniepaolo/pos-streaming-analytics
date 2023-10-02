import logging
import logging.config
from pathlib import Path

if logging.getLogger().hasHandlers():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
else:
    logging.config.fileConfig(
        fname=Path("config/logging.properties"),
        disable_existing_loggers=False,
    )
    logger = logging.getLogger(__name__)
