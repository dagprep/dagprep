from typing import Callable, Dict, List, Tuple


class Transformation:
    def __init__(self, 
        function_: Callable, 
        name: str = None, 
        notes: str = None
    ) -> None:
        self.function_ = function_
        self.name = name or function_.__name__
        self.depends_on: Dict[str, "Transformation"] = {}
        self.successors: Dict[str, "Transformation"] = {}
        self.output = None
        self.notes = notes

    def chain(self, 
        transformation: "Transformation", 
        param_key: str
    ) -> "Transformation":
        if isinstance(transformation, Callable):
            transformation = Transformation(transformation)

        self.successors[param_key] = transformation
        transformation.depends_on[param_key] = self
        return transformation

    def merge(self, 
        transformations: List[Tuple["Transformation", str]]
    ) -> "Transformation":
        for tf, param_key in transformations:
            tf.successors[param_key] = self
            self.depends_on[param_key] = tf

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
