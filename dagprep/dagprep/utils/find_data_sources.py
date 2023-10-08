import logging
from typing import List
from dagprep.core.base.data_source import DataSource
from dagprep.core.base.transformation import Transformation

logger = logging.getLogger(__name__)


def __dfs(tf: Transformation) -> List[DataSource]:
    if isinstance(tf, DataSource):
        return [tf]

    if len(tf.depends_on) == 0:
        logger.error(f"A transformation must have at least one dependency\n {tf}")
        raise ValueError("A transformation must have at least one dependency")

    dss = []
    for tf_dependency in tf.depends_on.values():
        dss += __dfs(tf_dependency)

    return dss


def find_data_sources(tfs: List[Transformation]) -> List[DataSource]:
    dss = []

    for tf in tfs:
        dss += __dfs(tf)

    return dss
