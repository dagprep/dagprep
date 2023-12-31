import pandas as pd


def fullname(worker_df):
    worker_df["Fullname"] = worker_df["Name"] + " " + worker_df["Surname"]
    return worker_df

def minmax(df_worker):
    df_worker["SalaryNormalized"]  = (df_worker["Salary"] - df_worker["Salary"].min()) / (df_worker["Salary"].max() - df_worker["Salary"].min()) 
    return df_worker

def upper_col(companies_df, col_to_upper):
    upper_col_new_name = f"{col_to_upper}Upper"
    companies_df[upper_col_new_name] = companies_df[col_to_upper].str.upper()
    return companies_df


def add_companies_info(workers_df, companies_df): 
    return pd.merge(workers_df, companies_df, left_on=["CompanyId"], right_on=["Id"], how="left")

def add_cities_info(workers_df, cities_df):
    return pd.merge(workers_df, cities_df, left_on=["CityId"], right_on=["Id"], how="left")

def add_info(workers_df, companies_df, cities_df): 
    workers_df = pd.merge(workers_df, companies_df, left_on=["CompanyId"], right_on=["Id"], how="left")
    return pd.merge(workers_df, cities_df, left_on=["CityId"], right_on=["Id"], how="left")

def select_cols(workers_companies_df):
    cols_to_keep = ['Salary', 'CompanyId', 'Fullname', 'SalaryNormalized', 'Name_y', 'NameUpper', "CityName"]
    return workers_companies_df[cols_to_keep]

def identity_function(workers_companies_df):
    return workers_companies_df
