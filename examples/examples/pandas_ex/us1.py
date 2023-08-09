import pandas as pd

from examples.pandas_ex import COMPANIES_DF_LOCATION, WORKER_DF_LOCATION
from examples.pandas_ex.transformations import fullname, minmax, upper_col, add_companies_info, select_cols


if __name__ == '__main__':
    companies_df = pd.read_csv(COMPANIES_DF_LOCATION, index_col="Id")
    worker_df = pd.read_csv(WORKER_DF_LOCATION, index_col="Id")
    fullname(worker_df)
    minmax(worker_df)
    # print(worker_df)
    upper_col(companies_df)
    # print(companies_df)
    workers_companies_df = add_companies_info(worker_df, companies_df)
    out_df = select_cols(workers_companies_df)
    print(out_df)

