import logging
from typing import Any

from dagprep.pipeline.action_fn import exec, visit
from dagprep.pipeline.pipeline_explorer import PipelineExplorer
from dagprep.pipeline.steps.data_source import DataSource

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, data_sources: list[DataSource]):
        self.data_sources = data_sources
        self.pe = PipelineExplorer(self.data_sources)

    def exec(self) -> dict[str, Any]:
        return self.pe.explore(exec, {})

    def get_execution_plan(self) -> list[str]:
        return self.pe.explore(visit, [])
