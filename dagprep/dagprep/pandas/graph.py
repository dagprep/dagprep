import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

from dagprep.pandas.us1 import add_companies_info, fullname, upper_col, minmax

def create_dag(worker_df, companies_df):
    G = nx.DiGraph()

    # INPUT NODES
    G.add_node("Workers", output=worker_df)
    
    # G.add_node("Companies", output=companies_df)

    # FUNCTION NODES
    G.add_node("fullname", function=fullname)
    G.add_node("minmax", function=minmax)
    # G.add_node("upper_col", function=upper_col)
    # G.add_node("add_companies_info", function=add_companies_info)  
    G.add_node("output", function= lambda x: x)

    # EDGEs
    G.add_edge("Workers", "fullname")
    G.add_edge("fullname", "minmax")
    # G.add_edge("Companies", "upper_col")
    # G.add_edge("minmax", "add_companies_info")
    # G.add_edge("upper_col", "add_companies_info")

    # G.add_edge("add_companies_info", "output")
    G.add_edge("minmax", "output")

    return G
    
def draw_dag(G):
    # Create the figure and axis
    fig, ax = plt.subplots()

    # Draw the graph nodes with labels
    pos = nx.spring_layout(G)  # You can use different layout algorithms here
    nx.draw_networkx_nodes(G, pos, ax=ax)
    nx.draw_networkx_labels(G, pos, labels={node: node for node in G.nodes()}, ax=ax)

    # Draw the edges
    nx.draw_networkx_edges(G, pos, ax=ax)

    # Save the graph visualization to a file
    fig.savefig("graph.png")

    # Display the graph visualization
    plt.show()

if __name__ == '__main__':
    companies_df = pd.read_csv("/home/giambrosio/projects/personal/dagprep/dagprep/dagprep/pandas/us1/data/companies.csv", index_col="Id")
    worker_df = pd.read_csv("/home/giambrosio/projects/personal/dagprep/dagprep/dagprep/pandas/us1/data/worker.csv", index_col="Id")
    
    G = create_dag(worker_df, companies_df)

    for node_label in nx.topological_sort(G):

        if "output" not in G.nodes[node_label]:  
            print(f"Processing node {node_label}")
            nodes = [z for (z, u) in G.in_edges(node_label)]
            nodes_output = [G.nodes[node]["output"] for node in nodes]
            G.nodes[node_label]["output"] = G.nodes[node_label]["function"](*nodes_output)
            print(G.nodes[node_label]["output"])

    print(G.nodes["output"]["output"])
    # draw_dag(G)





