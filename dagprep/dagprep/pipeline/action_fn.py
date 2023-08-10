import logging
from typing import Any, Callable, TypeVar

from dagprep.pipeline.steps.transformation import Transformation

logger = logging.getLogger(__name__)


ActionParam = TypeVar("ActionParam", list[str], dict[str, Any])
ActionFn = Callable[[Transformation, ActionParam], None]


def exec(tf: Transformation, output: dict[str, Any]):
    logger.info("Processing node %s" % tf.name)
    tf.exec()
    if len(tf.successors) == 0:
        output[tf.name] = tf.output


def visit(tf: Transformation, topological_order: list[str]):
    logger.info("Visiting node %s" % tf.name)
    topological_order.append(tf.name)
