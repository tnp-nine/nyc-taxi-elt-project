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

def transform_client_data(input_paths):
    print('Transforming client data...')
    # initialize fake function - name, phone_number, and email based on UK
    fake = Faker('en_GB')

    # generate fake client data
    client_df = pd.read_parquet(input_paths['user_data'])
    client_df = client_df.drop_duplicates().reset_index(drop=True)
    client_df = client_df[client_df['User ID'] <= 700].copy()
    client_df = client_df.rename(columns={'User ID': 'client_id', 'Age': 'age', 'Gender':'gender'})
    client_df['name'] = generate_fake_info(fake.name, len(client_df))
    client_df['phone_number'] = generate_fake_info(fake.phone_number, len(client_df))
    client_df['email'] = generate_fake_info(fake.email, len(client_df))

    print('Client data transformation completed.')
    return client_df

def transform_driver_data(input_paths):
    print('Transforming driver data...')

    # initialize fake function - name, phone_number, and email based on UK
    fake = Faker('en_GB')

    # generate fake client data
    driver_df = pd.read_parquet(input_paths['user_data'])
    driver_df = driver_df.drop_duplicates().reset_index(drop=True)
    driver_df = driver_df[driver_df['User ID'] > 700].copy()
    driver_df = driver_df.rename(columns={'User ID': 'client_id', 'Age': 'age', 'Gender':'gender'})
    driver_df['name'] = generate_fake_info(fake.name, len(driver_df))
    driver_df['phone_number'] = generate_fake_info(fake.phone_number, len(driver_df))
    driver_df['email'] = generate_fake_info(fake.email, len(driver_df))

    print('Driver data transformation completed.')

def transform_vehicle_data(input_paths):
    print('Transforming vehicle data...')
    vehicle_df = pd.read_parquet(input_paths['vehicle_data'])

    # clean and format vehicle data
    vehicle_df = vehicle_df.drop_duplicates().reset_index(drop=True)
    # copy some data from original vehicle_df
    vehicle_df = vehicle_df[['Public Vehicle Number',
                             'Vehicle Make',
                             'Vehicle Model',
                             'Vehicle Model Year',
                             'Vehicle Color']].copy()
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
    
    print('Vehicle data transformation completed.')
    return vehicle_df

def transform_payment_data(input_paths):
    print('Transforming payment data...')
    taxi_files = input_paths['taxi_data']
    # read all file in folder
    taxi_df = pd.concat([pd.read_parquet(file) for file in taxi_files], ignore_index=True)
    # extract some data for payment information
    payment_df = taxi_df[['fare_amount','mta_tax',
                          'extra', 'improvement_surcharge',
                          'tip_amount', 'tolls_amount', 'Airport_fee',
                          'congestion_surcharge']].copy()
    # add payment_id for each record
    payment_df['payment_id'] = range(1, len(payment_df)+1)
    # rearrage column
    new_col = ['payment_id'] + [col for col in payment_df.columns if col != 'payment_id']
    payment_df = payment_df[new_col]
    
    print('Payment data transformation completed.')
    return payment_df

def transform_zone_data(input_paths):
    print('Transforming location data...')
    zone_df = pd.read_parquet(input_paths['zone_data'])

    # preprocessing data
    zone_df = zone_df.drop_duplicates().reset_index(drop=True)
    # Create zone dataset
    zone_df = zone_df[['LocationID', 'Borough', 'Zone']].copy()
    zone_df = zone_df.rename(columns={'LocationID': 'location_id'})

    print('Zone data transformation completed.')
    return zone_df

def transform_trip_data(file_paths):
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

# def create_trip_info(client_df, driver_df, taxi_df, zone_df, payment_df):
#     print('Creating trip information...')
#     trip_df = taxi_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID']].copy()
#     trip_df = trip_df.rename(columns={
#         'tpep_pickup_datetime':'pickup_datetime',
#         'tpep_dropoff_datetime': 'dropoff_datetime',
#         'DOLocationID':'DO_loc_id',
#         'PULocationID':'PU_loc_id'})
#     # assign payment_id for each trip
#     trip_df['payment_id'] = payment_df['payment_id']
#     # assign trip_id
#     trip_df['trip_id'] = range(1, len(trip_df)+1)
#     # random assign driver_id, client_id, and rating for each trip record
#     trip_df['driver_id'] = np.random.choice(driver_df['driver_id'], size=len(trip_df))
#     trip_df['client_id'] = np.random.choice(client_df['client_id'], size=len(trip_df))
#     trip_df['rating'] = np.random.choice(['positive','negative','neutral'], size=len(trip_df))
#     # rearrage the columns
#     trip_df = trip_df[['trip_id', 'client_id', 'driver_id','payment_id', 'PU_loc_id', 'DO_loc_id', 'rating', 'pickup_datetime', 'dropoff_datetime']].copy()

#     print('Created trip information successfully.')
#     return trip_df

# def transform_all_data(bucket_name, file_paths):
#     print('Starting data transformation process...')
#     # load raw data
#     user_df = download_parquet_from_gcs(bucket_name, file_paths['user_data'])
#     taxi_df = download_parquet_from_gcs(bucket_name, file_paths["trip_data"])
#     vehicle_df = download_parquet_from_gcs(bucket_name, file_paths["vehicle_data"])
#     zone_df = download_parquet_from_gcs(bucket_name, file_paths["zone_data"])

#     # transform data
#     client_table, driver_table = transform_user_data(user_df)
#     payment_table = extract_payment_info(taxi_df)
#     vehicle_table = assign_driver_to_vehicle(vehicle_df)
#     location_table = extract_location_info(zone_df)

#     # generate trip table
#     trip_table = create_trip_info(client_table, driver_table, taxi_df, location_table, payment_table)

#     print('Data transformation process completed successfully.')
#     return client_table, driver_table, vehicle_table, location_table, trip_table, payment_table


