import pandas as pd
import numpy as np
from urllib import request
from io import StringIO
from sqlalchemy import create_engine, TEXT, FLOAT, BIGINT

# Download CSV file
url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"
response = request.urlopen(url)
data = response.read().decode('utf-8')

#create dataframe from the recieved data from file
df = pd.read_csv(StringIO(data), delimiter=';')

# Create a SQLite database engine
engine = create_engine(f'sqlite:///airports.sqlite')

# Define the column types for each column
column_types = {
        'column_1':BIGINT,
        'column_2':TEXT,
        'column_3':TEXT,
        'column_4':TEXT,
        'column_5':TEXT,
        'column_6':TEXT,
        'column_7':FLOAT,
        'column_8':FLOAT,
        'column_9':BIGINT,
        'column_10':FLOAT,
        'column_11':TEXT,
        'column_12':TEXT,
        'geo_punkt':TEXT
}

# Write the DataFrame to the SQLite database
df.to_sql('airports', engine, index=False, if_exists='replace', dtype=column_types)