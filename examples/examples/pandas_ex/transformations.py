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

def select_cols(workers_companies_df):
    cols_to_keep = ['Salary', 'CompanyId', 'Fullname', 'SalaryNormalized', 'Name_y', 'NameUpper']
    return workers_companies_df[cols_to_keep]

def identity_function(workers_companies_df):
    return workers_companies_df
