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

        self.depends_counter = 0

    def chain(self, transformation: Transformation, param_key: str) -> "Transformation":
        self.successors[param_key] = transformation
        transformation.depends_on[param_key] = self
        return transformation
    
    def is_ready(self):
        return True
    
    def visit(self):
        for _, out_trasformation in self.successors.items():
            out_trasformation.depends_counter += 1
    
    def exec(self):
        for _, out_trasformation in self.successors.items():
            out_trasformation.depends_counter += 1
        return self.data
