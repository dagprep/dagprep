import os
import pathlib as pl


PACKAGE_LOCATION = pl.Path(os.path.dirname(__file__))
ROOT_LOCATION = PACKAGE_LOCATION.parent

RESOURCE_SOURCE_LOCATION = PACKAGE_LOCATION / "resources"
