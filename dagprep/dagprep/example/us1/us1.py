import pandas as pd


def fullname(worker_df):
    worker_df["Fullname"] = worker_df["Name"] + " " + worker_df["Surname"]
    return worker_df

def minmax(df_worker):
    df_worker["SalaryNormalized"]  = (df_worker["Salary"] - df_worker["Salary"].min()) / (df_worker["Salary"].max() - df_worker["Salary"].min()) 
    return df_worker

def upper_col(companies_df):
    upper_col_new_name = "NameUpper"
    companies_df[upper_col_new_name] = companies_df["Name"].str.upper()
    return companies_df


def add_companies_info(workers_df, companies_df): 
    return pd.merge(workers_df, companies_df, left_on=["CompanyId"], right_on=["Id"], how="left")
    


if __name__ == '__main__':
    # companies_df = pd.read_csv("./us1/data/companies.csv", index_col="Id")
    # worker_df = pd.read_csv("./us1/data/worker.csv", index_col="Id")
    # fullname(worker_df)
    # minmax(worker_df)
    # # print(worker_df)
    # upper_col(companies_df, "Name")
    # # print(companies_df)
    # out_df = add_companies_info(worker_df, companies_df)
    # print(out_df)

    import inspect
    print(inspect.getfullargspec(add_companies_info).args)
