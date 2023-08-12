import logging
from typing import Any

from dagprep.pipeline.action_fn import exec, visit, visit_nx
from dagprep.pipeline.pipeline_explorer import PipelineExplorer
from dagprep.pipeline.steps.data import DataSource
from dagprep.pipeline.steps.transformation import Transformation

import networkx as nx

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, data_sources: list[DataSource]):
        self.data_sources = data_sources

    def exec(self) -> dict[str, Any]:
        pe = PipelineExplorer(self.data_sources)
        outputs = {}

        for tf in self.data_sources:
            pe.explore(tf, exec, outputs)

        return outputs

    def get_execution_plan(self) -> list[str]:
        pe = PipelineExplorer(self.data_sources)
        topological_order = []

        for tf in self.data_sources:
            pe.explore(tf, visit, topological_order)

        return topological_order

    def to_networkx(self) -> nx.DiGraph:
        g = nx.DiGraph()

        pe = PipelineExplorer(self.data_sources)

        for tf in self.data_sources:
            pe.explore(
                tf,
                visit_nx,
                g)

        return g