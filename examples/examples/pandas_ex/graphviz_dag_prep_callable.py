import pandas as pd
import dagprep as dp

from dagprep.utils.find_data_sources import find_data_sources

from examples.pandas_ex import (
    CITIES_DF_LOCATION,
    COMPANIES_DF_LOCATION,
    WORKER_DF_LOCATION,
)
from examples.pandas_ex.transformations import (
    add_info,
    fullname,
    minmax,
    select_cols,
    upper_col,
)

if __name__ == "__main__":
    worker_df = pd.read_csv(WORKER_DF_LOCATION, index_col="Id")
    worker_data = dp.DataSource(worker_df, "Workers dataframe")

    minmax_tf = worker_data.chain(fullname, "worker_df").chain(minmax, "df_worker")

    companies_df = pd.read_csv(COMPANIES_DF_LOCATION, index_col="Id")
    companies_data = dp.DataSource(companies_df, "Companies dataframe")

    upper_col_tf = dp.Transformation(upper_col, col_to_upper="Name")
    companies_data.chain(upper_col_tf, param_key="companies_df")

    cities_df = pd.read_csv(CITIES_DF_LOCATION, index_col="Id")
    cities_data = dp.DataSource(cities_df, "Cities dataframe")

    add_info_tf = dp.Transformation(add_info)
    minmax_tf.chain(add_info_tf, "workers_df")
    upper_col_tf.chain(add_info_tf, "companies_df")
    cities_data.chain(add_info_tf, "cities_df")


    output_pipeline = dp.Transformation(select_cols, name="output_pipeline")
    add_info_tf.chain(output_pipeline, param_key="workers_companies_df")

    pe = dp.Pipeline([worker_data, companies_data, cities_data])
    print(pe.get_execution_plan())
    print(pe.exec())

    g = pe.to_graphviz_digraph(name="DAG Example", filename="dag_example", format="png")
    g.render()

    data_sinks = [output_pipeline]
    data_sources = find_data_sources(data_sinks)
    print([ds.name for ds in data_sources])
