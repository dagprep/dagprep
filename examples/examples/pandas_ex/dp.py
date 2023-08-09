import pandas as pd
import logging
from examples.pandas_ex import COMPANIES_DF_LOCATION, WORKER_DF_LOCATION
from examples.pandas_ex.transformations import fullname, minmax, upper_col, add_companies_info
from dagprep.dp import DagPrep

logger = logging.getLogger(__name__)


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
