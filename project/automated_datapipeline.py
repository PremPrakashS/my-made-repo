import pandas as pd
from sqlalchemy import create_engine, TEXT, FLOAT, BIGINT
import numpy as np
import os

def create_date_table(start='2000-01-01', end='2050-12-31'):
    """
    Creates a DataFrame with a range of dates from 'start' to 'end', including various date components.
    
    Parameters:
    start (str): The start date for the range in 'YYYY-MM-DD' format. Default is '2000-01-01'.
    end (str): The end date for the range in 'YYYY-MM-DD' format. Default is '2050-12-31'.
    
    Returns:
    pd.DataFrame: A DataFrame containing columns for date, day, month, year, month name, weekday, 
                  weekday name, and quarter for each date in the range.
    """
    
    # Creating a DataFrame with a range of dates
    df = pd.DataFrame({"Date": pd.date_range(start, end)})

    # Extracting and adding date components as new columns
    df["Day"] = df.Date.dt.day.astype(str).str.zfill(2)
    df["Month"] = df.Date.dt.month.astype(str).str.zfill(2)
    df["Year"] = df.Date.dt.year.astype(np.int64)
    df["Month_Name"] = df.Date.dt.month_name()
    df["Weekday"] = df.Date.dt.weekday.astype(np.int64)
    df["Weekday_Name"] = df.Date.dt.day_name()
    df["Quarter"] = df.Date.dt.quarter.astype(np.int64)
    df['Month-Year'] = df['Year'].astype(str) + '-' + df['Month'].astype(str)
    df['Year-Quarter'] = df['Year'].astype(str) + '-Q' + df['Quarter'].astype(str)

    # Adjusting the date format if necessary (function call to 'fix_date_format')
    df = fix_date_format(df, ["Date"])

    return df


def get_source_data(path, delimiter):
    """
    Reads data from a specified file path into a pandas DataFrame.

    Parameters:
    path (str): The file path from which to read the data.
    delimiter (str): The delimiter used in the file (e.g., ',', ';', '\t', etc.).

    Returns:
    pd.DataFrame: A DataFrame containing the imported data.
    """

    # Using pandas to read data from the specified file path with the given delimiter
    df = pd.read_csv(path, delimiter=delimiter)

    return df


def rename_columns(df, col_dict):
    """
    Renames columns of a pandas DataFrame based on a provided dictionary mapping.

    Parameters:
    df (pd.DataFrame): The DataFrame whose columns are to be renamed.
    col_dict (dict): A dictionary mapping current column names to new column names.

    Returns:
    pd.DataFrame: The DataFrame with columns renamed as specified in the col_dict.
    """

    # Renaming columns of the DataFrame as per the provided dictionary mapping
    df.rename(columns=col_dict, inplace=True)

    return df



def drop_columns(df, col_list):
    """
    Removes specified columns from a pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame from which columns are to be removed.
    col_list (list): A list of strings representing the names of the columns to be removed.

    Returns:
    pd.DataFrame: The modified DataFrame with the specified columns removed.
    """

    # Dropping the specified columns from the DataFrame
    df.drop(col_list, axis=1, inplace=True)

    return df


def fix_date_format(df, col_list):
    """
    Adjusts the format of date columns in a pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the date columns to be formatted.
    col_list (list): A list of column names (strings) in the DataFrame which contain date information.

    Returns:
    pd.DataFrame: The DataFrame with adjusted date formats in the specified columns.
    """

    # Iterating over each column in the provided column list
    for col in col_list:
        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %I:%M:%S %p')
        df[col] = df[col].dt.strftime('%Y-%m-%d')    
    return df


def write_to_target(df, engine, table_name):
    """
    Writes a pandas DataFrame to a SQL table.

    Parameters:
    df (pd.DataFrame): The DataFrame to be written to the SQL table.
    engine: The SQLAlchemy engine instance used to connect to the database.
    table_name (str): The name of the target table in the SQL database where the data will be written.

    Note: Ensure that the connection specified by 'engine' is open and valid before calling this function.
    """

    # Writing the DataFrame to the specified SQL table without including the DataFrame's index
    df.to_sql(table_name, engine, index=False)

    

def transform_covid_data(df):
    """
    Transforms COVID-19 data in a pandas DataFrame.

    This function performs several operations on the DataFrame to prepare COVID-19 data for analysis:
    1. Deletes unnecessary columns.
    2. Renames columns for consistency and clarity.
    3. Adjusts the format of date columns.

    Parameters:
    df (pd.DataFrame): The DataFrame containing COVID-19 data to be transformed.

    Returns:
    pd.DataFrame: The transformed DataFrame with standardized column names and formats.
    """

    # Columns to be deleted from the DataFrame
    covid_data_col_del = ['county', 'state', 'Lat', 'Lon', 'fips', 'people_tested']

    # Dictionary for renaming columns
    covid_data_col_rename = {
        'date': 'Date',
        'cases': 'Cases_LA',
        'deaths': 'Deaths_LA',
        'state_cases': 'Cases_California',
        'state_deaths': 'Deaths_California',
        'new_cases': 'New_Cases_LA',
        'new_deaths': 'New_Deaths_LA',
        'new_state_cases': 'New_Cases_California',
        'new_state_deaths': 'New_Deaths_California'
    }

    # Columns containing date information
    date_columns = ['Date']
    
    # Dropping unnecessary columns
    df = drop_columns(df, covid_data_col_del)

    # Renaming columns for clarity
    df = rename_columns(df, covid_data_col_rename)

    # Fixing the date format
    df = fix_date_format(df, date_columns)
    
    columns_neg_val = ['New_Cases_LA',	'New_Deaths_LA', 'New_Cases_California', 'New_Deaths_California']
    df[columns_neg_val] = df[columns_neg_val].map(lambda x: abs(x) if x < 0 else x)
    
    return df


def transform_crime_data(df):
    """
    Transforms the structure and format of crime data in a pandas DataFrame.

    This function performs multiple operations on the crime data DataFrame:
    - Deletes unnecessary columns.
    - Renames columns for clarity and consistency.
    - Fixes the format of date columns.

    Parameters:
    df (pd.DataFrame): The DataFrame containing crime data.

    Returns:
    pd.DataFrame: The transformed DataFrame with cleaned and formatted crime data.
    """
    
    # List of columns to be deleted from the DataFrame
    crime_data_col_del = ['TIME OCC', 'Rpt Dist No', 'Part 1-2', 'Mocodes', 'Premis Cd', 'Premis Desc', 'Status', 'Status Desc', 'Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4']
    
    # Dictionary mapping old column names to new column names
    crime_data_col_rename = {
        'Date Rptd': 'Date_Reported',
        'DATE OCC': 'Date_Occured',
        'TIME OCC': 'Time_Occured',
        'AREA NAME': 'Area_Name',
        'Crm Cd': 'Crime_Code',
        'Crm Cd Desc': 'Crime_Code_Description',
        'Vict Age': 'Victim_Age',
        'Vict Sex': 'Victim_Sex',
        'Vict Descent': 'Victim_Descent',
        'Weapon Used Cd': 'Weapon_Code',
        'Weapon Desc': 'Weapon_Description',
        'LOCATION': 'Location',
        'Cross Street': 'Cross_Street',
        'LAT': 'Latitute',
        'LON': 'Longitude'
    }
    
    # Columns that contain date information
    date_columns = ['Date_Reported', 'Date_Occured']
       
    df = drop_columns(df, crime_data_col_del)
    df = rename_columns(df, crime_data_col_rename)
    df = fix_date_format(df, date_columns)  
    
    # Filtering for the crimes occoured in and after 2020
    # df = df[df['Year'] >= 2020]
    
    df.Weapon_Code = df.Weapon_Code.fillna(-1)
    df.Weapon_Code = df.Weapon_Code.astype(int)
    df.Cross_Street = df.Cross_Street.fillna('UNKNOWN')

    # Dictionary to map abbreviated gender codes to full descriptions    
    sex_dict = {
        'F' : 'FEMALE',
        'M' : 'MALE',
        'X' : 'UNKNOWN'
    }

    # Dictionary to map abbreviated descent codes to full descriptions
    descent_dict = {
        'A' : 'Other_Asian',
        'B' : 'Black',
        'C' : 'Chinese',
        'D' : 'Cambodian',
        'F' : 'Filipino',
        'G' : 'Guamanian',
        'H' : 'Hispanic/Latin/Mexican',
        'I' : 'American_Indian/Alaskan_Native',
        'J' : 'Japanese',
        'K' : 'Korean',
        'L' : 'Laotian',
        'O' : 'Other',
        'P' : 'Pacific_Islander',
        'S' : 'Samoan',
        'U' : 'Hawaiian',
        'V' : 'Vietnamese',
        'W' : 'White',
        'X' : 'Unknown',
        'Z' : 'Asian_Indian'
    }

    # Updating 'Victim_Sex' with full descriptions from the sex_dict   
    df.Victim_Sex = df.Victim_Sex.replace(sex_dict)

    # Creating a new column 'Victim_Descent_Desc' with full descriptions from the descent_dict    
    df['Victim_Descent_Desc'] = df['Victim_Descent'].map(descent_dict)   
    return df

    
def automated_data_pipeline(details):
    covid_df = get_source_data(details['covid_data']['source'], details['covid_data']['delimiter'])
    crime_df = get_source_data(details['crime_data']['source'], details['crime_data']['delimiter'])
    date_df = create_date_table(details['date_table']['start'], details['date_table']['end'])
    
    covid_df = transform_covid_data(covid_df)
    crime_df = transform_crime_data(crime_df)
    
    engine = create_engine(f"sqlite:///{details['target_db_path']}\\{details['target_db_name']}.db")
    
    write_to_target(covid_df, engine, details['covid_data']['target_table'])
    write_to_target(crime_df, engine, details['crime_data']['target_table'])
    write_to_target(date_df, engine, details['date_table']['target_table'])
    

if __name__ == '__main__':
    src_tgt_details = {
        'covid_data' : {
            'source' : "https://data.lacity.org/api/views/jsff-uc6b/rows.csv?accessType=DOWNLOAD",
            'delimiter' : ',',
            'target_table' : 'covid_data'
        },
        'crime_data' : {
            'source' : "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD",
            'delimiter' : ',',
            'target_table' : 'crime_data'
        },
        'date_table' : {
            'start':'2010-01-01',
            'end':'2023-12-31',
            'target_table' : 'Calender_data'
        },
        'target_db_path' : '.\\data',        
        'target_db_name' : 'made-project_new'
    }

    # automated_data_pipeline(details=src_tgt_details)
    covid_df = get_source_data(src_tgt_details['covid_data']['source'], src_tgt_details['covid_data']['delimiter'])
    crime_df = get_source_data(src_tgt_details['crime_data']['source'], src_tgt_details['crime_data']['delimiter'])
    date_df = create_date_table(src_tgt_details['date_table']['start'], src_tgt_details['date_table']['end'])
    
    covid_df = transform_covid_data(covid_df)
    crime_df = transform_crime_data(crime_df)
    
    engine = create_engine(f"sqlite:///{src_tgt_details['target_db_path']}\\{src_tgt_details['target_db_name']}.db")
    
    write_to_target(covid_df, engine, src_tgt_details['covid_data']['target_table'])
    write_to_target(crime_df, engine, src_tgt_details['crime_data']['target_table'])
    write_to_target(date_df, engine, src_tgt_details['date_table']['target_table'])
    
    engine.dispose()