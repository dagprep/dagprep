
import inspect
from typing import Callable


class Transformation():
    def __init__(self, 
        name: str, 
        function_: Callable, 
        depends_on: dict[str, "Transformation"] = None, 
        successors: dict[str, "Transformation"] = None, 
        notes: str = None
    ) -> None:
        self.name = name
        self.function_ = function_
        self.depends_on = depends_on or {}
        self.successors = successors or {}
        self.output = None
        self.notes = notes

        self.depends_counter = 0

    def chain(self, transformation: "Transformation", param_key: str) -> "Transformation":
        self.successors[param_key] = transformation
        transformation.depends_on[param_key] = self
        return transformation
    
    def merge(self, transformation1: "Transformation", param_key1: str, transformation2: "Transformation", param_key2: str):
        transformation1.successors[param_key1] = self
        transformation2.successors[param_key2] = self

        self.depends_on[param_key1] = transformation1
        self.depends_on[param_key2] = transformation2

        return self
    
    def is_ready(self):
        return self.depends_counter == len(self.depends_on)
    
    def visit(self):
        for _, out_trasformation in self.successors.items():
            out_trasformation.depends_counter += 1
   
    def exec(self):
        kwargs = {}
        for param_key, in_trasformation in self.depends_on.items():
            if in_trasformation.output is None:
                raise RuntimeError(
                    f"The transformation {self.name} cannot be executed before its dependency trasformation {in_trasformation.name}"
                )
            kwargs[param_key] = in_trasformation.output

        for _, out_trasformation in self.successors.items():
            out_trasformation.depends_counter += 1

        self.output = self.function_(**kwargs)
        return self.output



