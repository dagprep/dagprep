import logging
from typing import Any, Dict, List

import networkx as nx
from dagprep.pipeline.action_fn import exec, visit, visit_graphviz, visit_nx
from dagprep.pipeline.pipeline_explorer import PipelineExplorer
from dagprep.pipeline.steps.data_source import DataSource
from graphviz import Digraph

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, data_sources: List[DataSource]) -> None:
        self.data_sources = data_sources
        self.pe = PipelineExplorer(self.data_sources)

    def exec(self) -> Dict[str, Any]:
        return self.pe.explore(exec, {})

    def get_execution_plan(self) -> List[str]:
        return self.pe.explore(visit, [])

    def to_networkx(self) -> nx.DiGraph:
        return self.pe.explore(visit_nx, nx.DiGraph())

    def to_graphviz_digraph(self, **di_graph_kwargs) -> Digraph:
        dg = Digraph(**di_graph_kwargs)
        return self.pe.explore(visit_graphviz, dg)
