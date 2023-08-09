import logging
import copy

from dagprep.pipeline.steps.data import DataSource
from dagprep.pipeline.steps.transformation import Transformation


logger = logging.getLogger(__name__)


class Pipeline():
    def __init__(self, data_sources: list[DataSource]):
        self.data_sources = data_sources

    def exec(self):
        def dfs(transformation: Transformation):
            logger.info("Processing node %s" % transformation.name)
            processed.add(transformation)
            transformation.exec()
            if transformation.successors:
                for successor_tf in transformation.successors.values():
                    if successor_tf not in processed and successor_tf.is_ready():
                        dfs(successor_tf)
                    else:
                        logger.info(f"Adding {successor_tf.name} in to process set")
                        to_process.append(successor_tf)
            else:
                outputs.append(transformation.output)

        processed = set()
        to_process = copy.deepcopy(self.data_sources)
        outputs = []

        for tf in to_process:
            if tf not in processed:
                dfs(tf)

        return outputs
