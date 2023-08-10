from dagprep.pipeline.steps.transformation import Transformation


class DataSource():
    def __init__(self, 
        name: str, 
        data,
        successors: dict[str, Transformation] = None, 
        notes: str = None
    ) -> None:
        self.name = name
        self.data = data
        self.successors = successors or {}
        self.output = self.data
        self.notes = notes

    def chain(self, transformation: Transformation, param_key: str) -> "Transformation":
        self.successors[param_key] = transformation
        transformation.depends_on[param_key] = self
        return transformation
    
    def exec(self):
        return self.data
