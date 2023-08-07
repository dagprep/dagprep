import os
import pathlib as pl
import logging.config as logging_conf


PACKAGE_LOCATION = pl.Path(os.path.dirname(__file__))
ROOT_LOCATION = PACKAGE_LOCATION.parent

RESOURCES_LOCATION = PACKAGE_LOCATION / "resources"
LOGGING_CONF_LOCATION = RESOURCES_LOCATION / "logging.conf"

logging_conf.fileConfig(LOGGING_CONF_LOCATION)