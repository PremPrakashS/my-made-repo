import urllib.request
import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, TEXT, REAL, INTEGER


def extract_file_from_zip(zip_path, file_to_extract, path_to_extract_to = "./"):
    with zipfile.ZipFile(zip_path, 'r') as zip_file:
        zip_file.extract(file_to_extract, path_to_extract_to)
        
    return path_to_extract_to + file_to_extract


def get_source_zip(gtfs_url, gtfs_path):
    zip_filename, _ = urllib.request.urlretrieve(gtfs_url, gtfs_path)
    return zip_filename


def write_sql_to_df(db_name, table_name, column_dtype):
    engine = create_engine(f'sqlite:///{db_name}')
    stops_df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=column_dtype)
    engine.dispose()


def delete_files(file_list):
    for file in files_to_delete:
        if os.path.exists(file):
            os.remove(file)

if __name__ == '__main__':
    gtfs_url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
    gtfs_zip_path = "./GTFS.zip"
    file_to_extract = "stops.txt"
    files_to_delete = []
    db_name = "gtfs.sqlite"
    table_name = "stops"
    column_types = {
            'stop_id':INTEGER,
            'stop_name':TEXT,
            'stop_lat':REAL,
            'stop_lon':REAL,
            'zone_id':INTEGER
    }
    

    zip_path = get_source_zip(gtfs_url, gtfs_zip_path)

    file_path = extract_file_from_zip(zip_path, file_to_extract)

    stops_df = pd.read_csv(file_path, usecols=["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"])
    stops_df = stops_df[stops_df["zone_id"] == 2001]        # Filter for zone_id = 2001
    valid_coordinates = stops_df["stop_lat"].between(-90, 90) & stops_df["stop_lon"].between(-90, 90)    # filter for valid longitute and latitude
    stops_df = stops_df[valid_coordinates]
    stops_df.dropna(inplace=True)       # drop invalid rows

    write_sql_to_df(db_name, table_name, column_types)

    # Delete the stops.txt and GTFS.zip file
    files_to_delete.append(zip_path)
    files_to_delete.append(file_path)

    delete_files(files_to_delete)