from typing import Callable


class Transformation():
    def __init__(self, 
        function_: Callable, 
        name: str = None, 
        depends_on: dict[str, "Transformation"] = None, 
        successors: dict[str, "Transformation"] = None, 
        notes: str = None
    ) -> None:
        self.function_ = function_
        self.name = name or function_.__name__
        self.depends_on = depends_on or {}
        self.successors = successors or {}
        self.output = None
        self.notes = notes

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

   
    def exec(self):
        kwargs = {}
        for param_key, in_trasformation in self.depends_on.items():
            if in_trasformation.output is None:
                raise RuntimeError(
                    f"The transformation {self.name} cannot be executed before its dependency trasformation {in_trasformation.name}"
                )
            kwargs[param_key] = in_trasformation.output

        self.output = self.function_(**kwargs)
        return self.output



