import pandas as pd
from sqlalchemy import create_engine, TEXT, FLOAT, BIGINT


def create_date_table(start='2000-01-01', end='2050-12-31'):
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["Day"] = df.Date.dt.day
    df["Month"] = df.Date.dt.month
    df["Year"] = df.Date.dt.year
    df["Month_Name"] = df.Date.dt.month_name()
    df["Weekday"] = df.Date.dt.weekday
    df["Weekday_Name"] = df.Date.dt.day_name()
    df["Quarter"] = df.Date.dt.quarter
    return df


def get_source_data(path, delimiter):
    df = pd.read_csv(path)
    return df


def rename_columns(df, col_dict):
    df.rename(columns = col_dict, inplace = True)
    return df


def drop_columns(df, col_list):
    df.drop(col_list, axis=1, inplace=True)
    return df

def fix_date_format(df, col_list):
    for col in col_list:
        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %I:%M:%S %p')
        
    return df

def transform_covid_data(df):
    covid_data_col_del = ['county','state','Lat','Lon','fips','people_tested']
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
    date_columns = ['Date']
    
    df = df.drop_columns(covid_data_col_del, axis=1, inplace=True)
    df = df.rename_columns(df, covid_data_col_rename)
    df = df.fix_date_format(df, date_columns)
    
    return df

def transform_crime_data(df):
    crime_data_col_del = ['TIME OCC', 'Rpt Dist No', 'Part 1-2', 'Mocodes', 'Premis Cd', 'Premis Desc', 'Status', 'Status Desc', 'Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4']
    crime_data_col_rename = {
        'Date Rptd': 'Date_Reported',
        'DATE OCC': 'Date_Occured',
        'TIME OCC': 'Time_Occured',
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
    date_columns = ['Date_Reported', 'Date_Occured']
    
    df = df.drop_columns(crime_data_col_del, axis=1, inplace=True)
    df = df.rename_columns(df, crime_data_col_rename)
    df = df.fix_date_format(df, date_columns)
        
    df.Weapon_Code = df.Weapon_Code.fillna(-1)
    df.Weapon_Code = df.Weapon_Code.astype(int)
    df.Cross_Street = df.Cross_Street.fillna('UNKNOWN')
    
    sex_dict = {
        'F' : 'FEMALE',
        'M' : 'MALE',
        'X' : 'UNKNOWN'
    }

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
    
    df.Victim_Sex = df.Victim_Sex.replace(sex_dict)
    df['Victim_Descent_Desc'] = df['Victim_Descent'].map(descent_dict)   
    return df


def write_to_target(df, engine, table_name):
    df.to_sql(table_name, engine, index=False)
    
def automated_data_pipeline(details):
    covid_df = get_source_data(details['covid_data']['source'], details['covid_data']['delimiter'])
    crime_df = get_source_data(details['crime_data']['source'], details['crime_data']['delimiter'])
    date_df = create_date_table(details['date_info']['start'], details['date_info']['end'])
    
    covid_df = transform_covid_data(covid_df)
    crime_df = transform_crime_data(crime_df)
    
    engine = create_engine(f"sqlite:///{details['target_db_path']}\\{details['target_db_name']}.db")
    
    write_to_target(covid_df, engine, details['covid_data']['target_table'])
    write_to_target(crime_df, engine, details['crime_data']['target_table'])
    

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
        'date_info' : {
            'start':'2020-01-01',
            'end':'2021-12-31'
        },
        'target_db_path' : '..\\data',
        'target_db_name' : 'made-project01'
    }

automated_data_pipeline(details=src_tgt_details)