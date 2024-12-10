import pnadas as pd
from faker import Faker
import random
import numpy as np
from google.cloud import bigquery
from google.cloud import storage

def download_parquet_from_gcs(bucket_name, file_name):
    print(f'Downloading {file_name} from bucket {bucket_name}...')
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_byte()
    print(f'Downloaded {file_name} successfully.')
    return pd.read_parquet(pd.compat.BytesIO(data))

def generate_fake_info(func, n):
    generated = set()
    unique_value = []
    while len(unique_value) < n:
        value = func()
        # to make sure there are no duplicate value before adding
        if value not in generated:
            unique_value.append(value)
            generated.add(value)
    return unique_value

def transform_user_data(user_df):
    print('Transforming user data...')
    # initialize fake function - name, phone_number, and email based on UK
    fake = Faker('en_GB')
    # preprocessing data
    user_df = user_df.drop_duplicates().reset_index(drop=True)
    all_user_df = user_df[['User ID', 'Age', 'Gender']].copy()
    all_user_df = all_user_df.rename(columns={'Age': 'age', 'Gender': 'gender'})
    # create fake info
    all_user_df['name'] = generate_fake_info(fake.name, len(all_user_df))
    all_user_df['phone_number'] = generate_fake_info(fake.phone_number, len(all_user_df))
    all_user_df['email'] = generate_fake_info(fake.email, len(all_user_df))
    # create data for users
    client_df = all_user_df[(all_user_df['User ID'] >= 1) & (all_user_df['User ID'] <= 700)].copy()
    client_df = client_df.rename(columns={'User ID': 'client_id'})
    # create data for drivers
    driver_df = all_user_df[(all_user_df['User ID'] >= 701) & (all_user_df['User ID'] <= 1000)].copy()
    driver_df = driver_df.reset_index(drop=True)
    driver_df['User ID'] = range(1, len(driver_df)+1)
    driver_df = driver_df.rename(columns={'User ID': 'driver_id'})

    print('Transforming user data successfully.')
    return client_df, driver_df

def extract_payment_info(taxi_df):
    print('Extracting payment information...')
    # copy payment info from taxi data
    payment_df = taxi_df[['fare_amount','mta_tax', 'extra', 'improvement_surcharge', 'tip_amount', 'tolls_amount', 'Airport_fee', 'congestion_surcharge']].copy()
    # add payment_id for each record
    payment_df['payment_id'] = range(1, len(payment_df)+1)
    # rearrage column
    new_col = ['payment_id'] + [col for col in payment_df.columns if col != 'payment_id']
    payment_df = payment_df[new_col]
    
    print('Extracted payment information successfully.')
    return payment_df

def assign_driver_to_vehicle(driver_df, vehicle_df):
    print('Assigning drivers to vehicle...')
    vehicle_df = vehicle_df.drop_duplicates().reset_index(drop=True)
    # copy some data from original vehicle_df
    vehicle_df = vehicle_df[['Public Vehicle Number', 'Vehicle Make', 'Vehicle Model', 'Vehicle Model Year', 'Vehicle Color']].copy()
    # rename columns
    vehicle_df = vehicle_df.rename(columns={
        'Public Vehicle Number': 'vehicle_number',
        'Vehicle Make':'vehicle_make',
        'Vehicle Model': 'vehicle_model',
        'Vehicle Model Year': 'vehicle_model_year',
        'Vehicle Color': 'vehicle_color'})
    # correct the format for model_year to be int instead of float
    vehicle_df['vehicle_model_year'] = vehicle_df['vehicle_model_year'].astype(int)
    # Selecting number vehicle to match with the number of driver which is 300 records
    vehicle_df = vehicle_df.sample(n = 300, random_state=1).reset_index(drop=True)
    # Assign driver id to each vehicle
    vehicle_df['driver_id'] = range(1, len(vehicle_df)+1)
    
    print('Assigning driver to vehicle successfully.')
    return vehicle_df

def extract_location_info(zone_df):
    print('Extracting location...')
    zone_df = zone_df.drop_duplicates().reset_index(drop=True)
    # Create zone dataset
    zone_df = zone_df[['LocationID', 'Borough', 'Zone']].copy()
    zone_df = zone_df.rename(columns={'LocationID': 'location_id'})

    print('Extractied location succesfully.')
    return zone_df

def create_trip_info(client_df, driver_df, taxi_df, zone_df, payment_df):
    print('Creating trip information...')
    trip_df = taxi_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID']].copy()
    trip_df = trip_df.rename(columns={
        'tpep_pickup_datetime':'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'DOLocationID':'DO_loc_id',
        'PULocationID':'PU_loc_id'})
    # assign payment_id for each trip
    trip_df['payment_id'] = payment_df['payment_id']
    # assign trip_id
    trip_df['trip_id'] = range(1, len(trip_df)+1)
    # random assign driver_id, client_id, and rating for each trip record
    trip_df['driver_id'] = np.random.choice(driver_df['driver_id'], size=len(trip_df))
    trip_df['client_id'] = np.random.choice(client_df['client_id'], size=len(trip_df))
    trip_df['rating'] = np.random.choice(['positive','negative','neutral'], size=len(trip_df))
    # rearrage the columns
    trip_df = trip_df[['trip_id', 'client_id', 'driver_id','payment_id', 'PU_loc_id', 'DO_loc_id', 'rating', 'pickup_datetime', 'dropoff_datetime']].copy()

    print('Created trip information successfully.')
    return trip_df

def transform_all_data(bucket_name, file_paths):
    print('Starting data transformation process...')
    # load raw data
    user_df = download_parquet_from_gcs(bucket_name, file_paths['user_data'])
    taxi_df = download_parquet_from_gcs(bucket_name, file_paths["trip_data"])
    vehicle_df = download_parquet_from_gcs(bucket_name, file_paths["vehicle_data"])
    zone_df = download_parquet_from_gcs(bucket_name, file_paths["zone_data"])

    # transform data
    client_table, driver_table = transform_user_data(user_df)
    payment_table = extract_payment_info(taxi_df)
    vehicle_table = assign_driver_to_vehicle(vehicle_df)
    location_table = extract_location_info(zone_df)

    # generate trip table
    trip_table = create_trip_info(client_table, driver_table, taxi_df, location_table, payment_table)

    print('Data transformation process completed successfully.')
    return client_table, driver_table, vehicle_table, location_table, trip_table, payment_table


