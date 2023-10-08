from typing import Callable, Dict
from dagprep.pipeline.steps.transformation import Transformation


class DataSource:
    def __init__(self, 
        name: str, 
        data,
        notes: str = None
    ) -> None:
        self.name = name
        self.data = data
        self.successors: Dict[str, "Transformation"] = {}
        self.output = self.data
        self.notes = notes

    def chain(self, transformation: Transformation, param_key: str) -> "Transformation":
        if isinstance(transformation, Callable):
            transformation = Transformation(transformation)

        self.successors[param_key] = transformation
        transformation.depends_on[param_key] = self
        return transformation
    
    def exec(self):
        return self.data
