import logging
import copy

from dagprep.pipeline.transformation import Transformation


logger = logging.getLogger(__name__)


class Pipeline():
    def __init__(self, transformations: list[Transformation]):
        self.transformations = transformations


    def _exec_successors(self, transformation: Transformation):
        res = transformation.exec()
        if len(transformation.successors):
            for param_key, tf in transformation.successors.items():
                logger.info(f"exec transformation: {tf.name}")
                res = self._exec_successors(tf)
        return res

    def exec(self):
        def dfs(node):
            logger.info("Processing node %s" % node.name)
            processed.add(node)
            node.exec()
            if node.successors:
                for successor_tf in node.successors.values():
                    if successor_tf not in processed and successor_tf.is_ready():
                        dfs(successor_tf)
                    else:
                        logger.info(f"Adding {successor_tf.name} in to process set")
                        to_process.append(successor_tf)
            else:
                outputs.append(node.output)

        processed = set()
        to_process = copy.deepcopy(self.transformations)
        outputs = []

        for tf in to_process:
            if tf not in processed:
                dfs(tf)

        return outputs
 
    def get_execution_plan(self):
        def dfs(node):
            logger.info("Visiting node %s" % node.name)
            processed.add(node)
            topological_sort.append(node.name)
            node.visit()
            for successor_tf in node.successors.values():
                if successor_tf not in processed and successor_tf.is_ready():
                    dfs(successor_tf)
                else:
                    logger.info(f"Adding {successor_tf.name} in to process set")
                    to_process.append(successor_tf)


        processed = set()
        to_process = copy.deepcopy(self.transformations)
        topological_sort = []

        for tf in to_process:
            if tf not in processed:
                dfs(tf)

        return topological_sort
