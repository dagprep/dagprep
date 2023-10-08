import networkx as nx
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import dagprep
import logging

logger = logging.getLogger(__name__)


def dfs_with_functions(dag, node, results, executor):
    if node in results:
        return results[node].result()
    
    kwargs = {}
    for predecessor in dag.predecessors(node):
        kwargs[dag.edges[(predecessor, node)]["param_key"]] = dfs_with_functions(dag, predecessor, results, executor)

    _function = dag.nodes[node]["function"]
    results[node] = executor.submit(_function, **kwargs)

    return results[node]

def traverse_dag_with_functions_parallel(dag, n_threads=8):
    # Dictionary to store the results of completed tasks
    results = {}

    import time

    start = time.perf_counter()

    # ThreadPoolExecutor
    with ProcessPoolExecutor(max_workers=n_threads) as executor:
        # Start the DFS with functions execution from each node
        for start_node in ["B1", "B2", "B3", "B4", "B", "C", "A", "D"]:
            # Start DFS from the nodes with no predecessors (roots of DAG)
            dfs_with_functions(dag, start_node, results, executor)

        # Process the final results or perform other operations if needed
        for node, future in results.items():
            logger.info(f"Processing node: {node}")
            logger.info(f"Function result: {future.result()}")
            # Perform any other operations as needed
    end = time.perf_counter()

    return end-start


def waste_cpy_res():
    i = 0
    for _ in range(100_000_000):
        i+=1

import time

def bi_func():
    logger.info("Processing bi")
    waste_cpy_res()
    return "Function Bi executed"


def b_func(input_b1, input_b2, input_b3, input_b4):
    logger.info("Processing b")
    waste_cpy_res()
    return "Function B executed"


def c_func():
    logger.info("Processing c")
    waste_cpy_res()
    return "Function C executed"

def a_func(input_b, input_c):
    logger.info("Processing a")
    waste_cpy_res()
    return f"Function A executed with inputs {input_b} and {input_c}"

def d_func(input_a, input_c):
    logger.info("Processing d")
    waste_cpy_res()
    return f"Function A executed with inputs {input_a} and {input_c}"

# Example usage
dagprep_ = nx.DiGraph()
dagprep_.add_node("A", function=a_func)
dagprep_.add_node("B1", function=bi_func)
dagprep_.add_node("B2", function=bi_func)
dagprep_.add_node("B3", function=bi_func)
dagprep_.add_node("B4", function=bi_func)
dagprep_.add_node("B", function=b_func)
dagprep_.add_node("C", function=c_func)
dagprep_.add_node("D", function=d_func)

dagprep_.add_edge("B1", "B", param_key="input_b1")
dagprep_.add_edge("B2", "B", param_key="input_b2")
dagprep_.add_edge("B3", "B", param_key="input_b3")
dagprep_.add_edge("B4", "B", param_key="input_b4")
dagprep_.add_edge("B", "A", param_key="input_b")
dagprep_.add_edge("C", "A", param_key="input_c")
dagprep_.add_edge("A", "D", param_key="input_a")
dagprep_.add_edge("C", "D", param_key="input_c")


n_threads_list = [1, 2, 4, 8]
times_result = []
for n_threads in n_threads_list:
    time_ = traverse_dag_with_functions_parallel(dagprep_, n_threads=n_threads)
    times_result.append(time_)

logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

for t in times_result:
    logger.info(f"TIME: {t} using {t} threads")
 