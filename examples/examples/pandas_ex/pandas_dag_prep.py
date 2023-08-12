from examples.pandas_ex import CITIES_DF_LOCATION, WORKER_DF_LOCATION, COMPANIES_DF_LOCATION

import pandas as pd
from examples.pandas_ex.transformations import fullname, identity_function, minmax, select_cols, upper_col, add_companies_info, add_cities_info, add_info
from dagprep.pipeline.steps.data_source import DataSource
from dagprep.pipeline.pipeline import Pipeline
from dagprep.pipeline.steps.transformation import Transformation
from dagprep.pipeline.find_data_sources import find_data_sources


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

    # pp = Pipeline([worker_data, companies_data, cities_data])
    # print(pp.get_execution_plan())

    # print(pp.exec())

    data_sinks = [output_pipeline]
    data_sources = find_data_sources(data_sinks)
    print([ds.name for ds in data_sources])