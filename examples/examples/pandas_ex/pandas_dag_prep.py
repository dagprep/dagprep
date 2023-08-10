from examples.pandas_ex import WORKER_DF_LOCATION, COMPANIES_DF_LOCATION

import pandas as pd
from examples.pandas_ex.transformations import fullname, identity_function, minmax, select_cols, upper_col, add_companies_info
from dagprep.pipeline.steps.data import DataSource
from dagprep.pipeline.pipeline import Pipeline
from dagprep.pipeline.pipeline_explorer import PipelineExplorer
from dagprep.pipeline.steps.transformation import Transformation


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
    
    add_companies_info_tf = Transformation(add_companies_info)

    add_companies_info_tf.merge(
        upper_col_tf, "companies_df",
        minmax_tf, "workers_df"
    )

    select_cols_tf = Transformation(select_cols)
    output_pipeline = Transformation(identity_function, name="output_pipeline")
    (add_companies_info_tf
     .chain(select_cols_tf, param_key="workers_companies_df")
     .chain(output_pipeline, param_key="workers_companies_df"))
    
    # pipeline = Pipeline([worker_data, companies_data])
    # print(pipeline.exec())

    pe = Pipeline([worker_data, companies_data])
    print(pe.get_execution_plan())

    print(pe.exec())