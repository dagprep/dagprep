import logging
from typing import List

from dagprep.core.explorer.action_fn import ActionFn
from dagprep.core.explorer.counter_value import CounterValue
from dagprep.core.base.data_source import DataSource
from dagprep.core.base.transformation import Transformation

logger = logging.getLogger(__name__)


class PipelineExplorer:
    def __init__(self, data_sources: List[DataSource]) -> None:
        self.data_sources = data_sources

    def __explore(self, 
        tf: Transformation, 
        action_fn: ActionFn, 
        output_collector
    ) -> None:
        self.cv.count(tf.name)
        action_fn(tf, output_collector)
        for successor_tf in tf.successors.values():
            self.cv.count(successor_tf.name)
            if self.cv.is_ready(successor_tf):
                self.__explore(successor_tf, action_fn, output_collector)
            else:
                logger.info(
                    f"The transformation {successor_tf.name} is not ready to be explored"
                )

    def explore(self, 
        action_fn: ActionFn, 
        output_collector
    ):
        self.cv = CounterValue([ds.name for ds in self.data_sources])

        for ds in self.data_sources:
            self.__explore(ds, action_fn, output_collector)

        return output_collector
