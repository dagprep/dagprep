import logging

from dagprep.pipeline.action_fn import ActionFn
from dagprep.pipeline.counter_value import CounterValue
from dagprep.pipeline.steps.data import DataSource
from dagprep.pipeline.steps.transformation import Transformation

logger = logging.getLogger(__name__)


class PipelineExplorer:
    def __init__(self, data_sources: list[DataSource]):
        self.data_sources = data_sources
        self.cv = CounterValue([ds.name for ds in self.data_sources])

    def explore(self, tf: Transformation, action_fn: ActionFn, output_collector):
        self.cv.count(tf.name)
        action_fn(tf, output_collector)
        for successor_tf in tf.successors.values():
            self.cv.count(successor_tf.name)
            if self.cv.is_ready(successor_tf):
                self.explore(successor_tf, action_fn, output_collector)
            else:
                logger.info(
                    f"The transformation {successor_tf.name} is not ready to be explored"
                )