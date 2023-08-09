from typing import List, Callable
import networkx as nx
import pandas as pd
import inspect
import logging
from examples.pandas_ex import COMPANIES_DF_LOCATION, WORKER_DF_LOCATION
from examples.pandas_ex.transformations import fullname, minmax, upper_col, add_companies_info

logger = logging.getLogger(__name__)

OUTPUT_LABEL = "output"
PARAM_KEY_LABEL = "param_key"
OUTPUT_NODE_LABEL = "output"
FUNCTION_LABEL = "function"

FunctionSignature = Callable[[List[pd.DataFrame]], pd.DataFrame]

class DagPrep():
    def __init__(self) -> None:
        self._digraph = nx.DiGraph()
        self._digraph.add_node(OUTPUT_NODE_LABEL, function=lambda x: x)

    def add_source_node(self, label: str, data: pd.DataFrame) -> None:
        self._digraph.add_node(label, output=data)

    def add_processing_node(self, label: str, function: FunctionSignature) -> None:
        self._digraph.add_node(label, function=function)


    def add_edge(self, in_label: str, out_label: str, param_key: str):
        _function = self._digraph.nodes[out_label]["function"]
        if param_key not in inspect.getfullargspec(_function).args:
            err_msg = f"The processing node {out_label} doens't have an argument named {param_key}"
            logger.error(err_msg)
            raise ValueError(err_msg)
        self._digraph.add_edge(in_label, out_label, param_key=param_key)

    
    def __process_node(self, node_label: str):
        node = self._digraph.nodes[node_label]
        if OUTPUT_LABEL not in node:
            logger.info(f"Processing node {node_label}")
            kwargs = {self._digraph.edges[in_e][PARAM_KEY_LABEL]: self._digraph.nodes[in_e[0]][OUTPUT_LABEL] for in_e in self._digraph.in_edges(node_label)}
            node[OUTPUT_LABEL] = node[FUNCTION_LABEL](**kwargs)


    def execute(self) -> pd.DataFrame:
        for node_label_to_process in nx.topological_sort(self._digraph):
            self.__process_node(node_label_to_process)

        return self._digraph.nodes[OUTPUT_NODE_LABEL][OUTPUT_LABEL]
    

if __name__ == "__main__":
    companies_df = pd.read_csv(COMPANIES_DF_LOCATION, index_col="Id")
    worker_df = pd.read_csv(WORKER_DF_LOCATION, index_col="Id")
    
    dagprep = DagPrep()

    dagprep.add_source_node("Workers", data=worker_df)
    dagprep.add_source_node("Companies", data=companies_df)

    dagprep.add_processing_node("fullname", function=fullname)
    dagprep.add_processing_node("minmax", function=minmax)
    dagprep.add_processing_node("upper_col", function=upper_col)
    dagprep.add_processing_node("add_companies_info", function=add_companies_info)

    # EDGEs
    dagprep.add_edge("Workers", "fullname", param_key="worker_df")
    dagprep.add_edge("fullname", "minmax", param_key="df_worker")
    dagprep.add_edge("Companies", "upper_col", param_key="companies_df")
    dagprep.add_edge("minmax", "add_companies_info", param_key="workers_df")
    dagprep.add_edge("upper_col", "add_companies_info", param_key="companies_df")

    dagprep.add_edge("add_companies_info", "output", param_key="x")
    

    output = dagprep.execute()

    print(output)
