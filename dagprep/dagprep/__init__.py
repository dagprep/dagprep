import os
import pathlib as pl
import logging.config as logging_conf


PACKAGE_LOCATION = pl.Path(os.path.dirname(__file__))
ROOT_LOCATION = PACKAGE_LOCATION.parent

RESOURCES_LOCATION = PACKAGE_LOCATION / "resources"
LOGGING_CONF_LOCATION = RESOURCES_LOCATION / "logging.conf"

logging_conf.fileConfig(LOGGING_CONF_LOCATION)

from dagprep.core.base.data_source import DataSource
from dagprep.core.base.transformation import Transformation

from dagprep.core.explorer.pipeline_explorer import PipelineExplorer
from dagprep.core.explorer.action_fn import ActionFn, ActionParam

from dagprep.core.pipeline import Pipeline
