import os

from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def ingestion_data(table_name, parquet_file):
    print(table_name, parquet_file)


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('Connection established successfully, inserting data...')

    # Open the Parquet file using pyarrow
    parquet_file = pq.ParquetFile(parquet_file)

    # Start inserting data in batches (chunks)
    first_batch = True

    for batch in parquet_file.iter_batches(batch_size=100000):
        # Convert the pyarrow batch to a pandas DataFrame
        df = batch.to_pandas()

        # Convert datetime columns if necessary
        if 'tpep_pickup_datetime' in df.columns:
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        if 'tpep_dropoff_datetime' in df.columns:
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        # Insert the first chunk with table creation, others will append
        if first_batch:
            df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            first_batch = False

        # Insert chunk into the database
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        print(f"Inserted a batch of {len(df)} rows.")

    print("Finished ingesting data into the Postgres database.")
