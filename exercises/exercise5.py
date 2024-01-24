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


gtfs_url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
gtfs_zip_path = "./GTFS.zip"
file_to_extract = "stops.txt"

zip_path = get_source_zip(gtfs_url, gtfs_zip_path)

file_path = extract_file_from_zip(zip_path, file_to_extract)

stops_df = pd.read_csv(file_path, usecols=["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"])

# Filter for zone_id = 2001
stops_df = stops_df[stops_df["zone_id"] == 2001]
# for valid longitute and latitude
valid_coordinates = stops_df["stop_lat"].between(-90, 90) & stops_df["stop_lon"].between(-90, 90)
stops_df = stops_df[valid_coordinates]
#drop invalid rows
stops_df.dropna(inplace=True)

engine = create_engine(f'sqlite:///gtfs.sqlite')

column_types = {
        'stop_id':INTEGER,
        'stop_name':TEXT,
        'stop_lat':REAL,
        'stop_lon':REAL,
        'zone_id':INTEGER
}

stops_df.to_sql("stops", engine, if_exists="replace", index=False, dtype=column_types)
engine.dispose()

# Delete the stops.txt and GTFS.zip file
files_to_delete = []
files_to_delete.append(zip_path)
files_to_delete.append(file_path)

for file in files_to_delete:
    if os.path.exists(file):
        os.remove(file)