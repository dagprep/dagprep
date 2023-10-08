import logging
from typing import Any, Callable, Dict, List, TypeVar

import networkx as nx
from dagprep.core.base.transformation import Transformation
from graphviz import Digraph

logger = logging.getLogger(__name__)


ActionParam = TypeVar("ActionParam", List[str], Dict[str, Any], nx.DiGraph, Digraph)
ActionFn = Callable[[Transformation, ActionParam], None]


def exec(tf: Transformation, output: Dict[str, Any]) -> None:
    logger.info("Processing node %s" % tf.name)
    tf.exec()
    if len(tf.successors) == 0:
        output[tf.name] = tf.output


def visit(tf: Transformation, topological_order: List[str]) -> None:
    logger.info("Visiting node %s" % tf.name)
    topological_order.append(tf.name)


def visit_nx(tf: Transformation, g: nx.DiGraph) -> None:
    g.add_node(tf.name)
    if isinstance(tf, Transformation):
        for dependency in tf.depends_on.values():
            g.add_edge(dependency.name, tf.name)


def visit_graphviz(tf: Transformation, dg: Digraph) -> None:
    if isinstance(tf, Transformation) and len(tf.successors) > 0:
        shape = "box" 
    else:
        shape = "ellipse"
    
    dg.node(tf.name, tf.name, shape=shape)
    if isinstance(tf, Transformation):
        for dependency in tf.depends_on.values():
            dg.edge(dependency.name, tf.name)
