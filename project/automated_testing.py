import pandas as pd
import sqlite3
from automated_datapipeline import automated_data_pipeline, create_date_table
from pandas.testing import assert_frame_equal

def check_date_format(df, date_col):
    date_format = '%Y-%m-%d'
    
    df['parsed_date'] = pd.to_datetime(df[date_col], format=date_format, errors='coerce')
    df['is_valid_date'] = ~df['parsed_date'].isna()
    
    assert any(not element for element in df['is_valid_date'].unique()) == False, f"Date format is not correct, {date_col} should be of format: '%Y-%m-%d'"
    
    
def check_num_cols(df, num):
    assert df.shape[1] == num, f"Number of column should be {num}" 
    
    
def check_column_names(df, expected_column_names):
    for x, y in zip(expected_column_names, df.columns):
        assert x == y, f"column name incorrect: {y} instead of {x}"
        
        
def check_null_values(df, cols):
    for col in cols:
        assert df[col].isna().any() == False, f'Column {col} contains null values'
        

def compare_dataframes(df1, df2):
    assert_frame_equal(df1, df2)    


def check_crime_data_table(df):
    check_num_cols(df, 17)
    crime_data_expected_columns = ['DR_NO', 'Date_Reported', 'Date_Occured', 'AREA', 'AREA NAME',
       'Crime_Code', 'Crime_Code_Description', 'Victim_Age', 'Victim_Sex',
       'Victim_Descent', 'Weapon_Code', 'Weapon_Description', 'Location',
       'Cross_Street', 'Latitute', 'Longitude', 'Victim_Descent_Desc']
    
    check_column_names(df, crime_data_expected_columns)
    
    date_columns = ['Date_Reported', 'Date_Occured']
    for col in date_columns:
        check_date_format(df, col)
        
    cols_to_check_null = ['Date_Reported', 'Date_Occured', 'Weapon_Code', 'Cross_Street']
    check_null_values(df, cols_to_check_null)

        
def check_covid_data_table(df):
    check_num_cols(df, 9)
    covid_data_expected_columns = ['Date', 'Cases_LA', 'Deaths_LA', 'Cases_California',
       'Deaths_California', 'New_Cases_LA', 'New_Deaths_LA',
       'New_Cases_California', 'New_Deaths_California']
    
    check_column_names(df, covid_data_expected_columns)
    
    date_columns = ['Date']
    for col in date_columns:
        check_date_format(df, col)
        
    cols_to_check_null = ['Date']
    check_null_values(df, cols_to_check_null)
    

def check_calender_data_table(df):
    check_num_cols(df, 8)
    
    date_columns = ['Date']
    for col in date_columns:
        check_date_format(df, col)
        
    cols_to_check_null = ['Date']
    check_null_values(df, cols_to_check_null)

    
def test_load(details):
    date_df = create_date_table(details['date_table']['start'], details['date_table']['end'])
    
    sql_path = f"{details['target_db_path']}\\{details['target_db_name']}.db"
    conn = sqlite3.connect(sql_path)
    date_table = pd.read_sql_query("SELECT * FROM Calender_data", conn)

    compare_dataframes(date_df, date_table)
    
    conn.close()

        
def read_sql_table(db_path, table_name):
    query = f"SELECT * FROM {table_name}"
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    return df

        
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
            'start':'2020-01-01',
            'end':'2021-12-31',
            'target_table' : 'calender_data'
        },
        'target_db_path' : '..\\data',
        
        'target_db_name' : 'made-project_1'
    }

    automated_data_pipeline(details=src_tgt_details)
    
    test_load(src_tgt_details)
        
    db_path = f"{src_tgt_details['target_db_path']}\\{src_tgt_details['target_db_name']}.db"
    crime_data = read_sql_table(db_path, src_tgt_details['crime_data']['target_table'])
    covid_data = read_sql_table(db_path, src_tgt_details['covid_data']['target_table'])
    calender_data = read_sql_table(db_path, src_tgt_details['date_table']['target_table'])
    
    check_calender_data_table(calender_data)    
    check_crime_data_table(crime_data)
    check_covid_data_table(covid_data)