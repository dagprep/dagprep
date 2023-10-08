from typing import TypeVar, Generic, Callable, Dict
from dagprep.core.base.transformation import Transformation

T = TypeVar('T')

class DataSource(Generic[T]):
    def __init__(self, 
        data: T,
        name: str = None, 
        notes: str = None
    ) -> None:
        self.data = data
        self.name = name or data.__name__
        self.successors: Dict[str, "Transformation"] = {}
        self.output = self.data
        self.notes = notes

    def chain(self, transformation: Transformation, param_key: str) -> "Transformation":
        if isinstance(transformation, Callable):
            transformation = Transformation(transformation)

        self.successors[param_key] = transformation
        transformation.depends_on[param_key] = self
        return transformation
    
    def exec(self) -> T:
        return self.data
   