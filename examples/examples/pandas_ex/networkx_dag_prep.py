from examples.pandas_ex import CITIES_DF_LOCATION, WORKER_DF_LOCATION, COMPANIES_DF_LOCATION

import pandas as pd
from examples.pandas_ex.transformations import fullname, identity_function, minmax, select_cols, upper_col, add_companies_info, add_cities_info, add_info
from dagprep.pipeline.steps.data_source import DataSource
from dagprep.pipeline.pipeline import Pipeline
from dagprep.pipeline.pipeline_explorer import PipelineExplorer
from dagprep.pipeline.steps.transformation import Transformation

import networkx as nx
import matplotlib.pyplot as plt

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
    worker_df = pd.read_csv(WORKER_DF_LOCATION, index_col="Id")
    worker_data = DataSource("Workers dataframe", worker_df)
    fullname_tf = Transformation(fullname)
    minmax_tf = Transformation(minmax)
    (worker_data
    .chain(fullname_tf, "worker_df")
    .chain(minmax_tf, "df_worker"))

    companies_df = pd.read_csv(COMPANIES_DF_LOCATION, index_col="Id")
    companies_data = DataSource("Companies dataframe", companies_df)
    upper_col_tf = Transformation(upper_col)
    companies_data.chain(upper_col_tf, param_key="companies_df")

    cities_df = pd.read_csv(CITIES_DF_LOCATION, index_col="Id")
    cities_data = DataSource("Cities dataframe", cities_df)
    
    add_info_tf = Transformation(add_info)
    add_info_tf.merge(
        [(upper_col_tf, "companies_df"),
         (minmax_tf, "workers_df"),
         (cities_data, "cities_df")]
    )

    select_cols_tf = Transformation(select_cols)
    output_pipeline = Transformation(identity_function, name="output_pipeline")
    (add_info_tf
     .chain(select_cols_tf, param_key="workers_companies_df")
     .chain(output_pipeline, param_key="workers_companies_df"))
    
    # pipeline = Pipeline([worker_data, companies_data])
    # print(pipeline.exec())

    pe = Pipeline([worker_data, companies_data, cities_data])
    #print(pe.get_execution_plan())

    g = pe.to_networkx()
    draw_dag(g)

    #print(pe.exec())