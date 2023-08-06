import pandas as pd


def fullname(worker_df):
    worker_df["Fullname"] = worker_df["Name"] + " " + worker_df["Surname"]

def minmax(df_worker):
    df_worker["SalaryNormalized"]  = (df_worker["Salary"] - df_worker["Salary"].min()) / (df_worker["Salary"].max() - df_worker["Salary"].min()) 

def upper_col(companies_df, col_to_capitalize):
    upper_col_new_name = f"{col_to_capitalize}Upper"
    companies_df[upper_col_new_name] = companies_df[col_to_capitalize].str.upper()


def add_companies_info(workers_df, companies_df): 
    return pd.merge(workers_df, companies_df, left_on=["CompanyId"], right_on=["Id"], how="left")
    


if __name__ == '__main__':
    companies_df = pd.read_csv("/home/giambrosio/projects/personal/dagprep/dagprep/dagprep/pandas/us1/data/companies.csv", index_col="Id")
    worker_df = pd.read_csv("/home/giambrosio/projects/personal/dagprep/dagprep/dagprep/pandas/us1/data/worker.csv", index_col="Id")
    fullname(worker_df)
    minmax(worker_df)
    # print(worker_df)
    upper_col(companies_df, "Name")
    # print(companies_df)
    out_df = add_companies_info(worker_df, companies_df)
    print(out_df)

