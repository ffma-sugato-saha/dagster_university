import requests
from . import constants
from dagster import asset,AssetExecutionContext,MaterializeResult,MetadataValue
# import duckdb
# import os
import pandas as pd
from dagster_duckdb import DuckDBResource
from ..partitions import monthly_partition

@asset (
  partitions_def=monthly_partition,
  group_name = "raw_files"
)
def taxi_trips_file(context: AssetExecutionContext) -> MaterializeResult:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    raw_trips = requests.get(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet") 
    
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch),"wb") as output_file:
      output_file.write(raw_trips.content)

    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))
    return MaterializeResult(
    metadata={
        'Number of records': MetadataValue.int(num_rows)
      }
    )

@asset (
    group_name = "raw_files"
)
def taxi_zones_file() -> MaterializeResult:
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get("https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD")

    with open(constants.TAXI_ZONES_FILE_PATH,"wb" ) as output_file:
      output_file.write(raw_taxi_zones.content)

    num_rows = len(pd.read_csv(constants.TAXI_ZONES_FILE_PATH))
    return MaterializeResult(
    metadata={
        'Number of records': MetadataValue.int(num_rows)
      }
    )

@asset(deps=["taxi_trips_file"],
      partitions_def=monthly_partition,
      group_name = "ingested"
)
def taxi_trips(context: AssetExecutionContext, database: DuckDBResource) -> None:
    partition_date_str = context.partition_key
    month_to_load = partition_date_str[:-3]
    """
      Create an empty target table
    """
    create_sql_query = """
        create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
      );
    """
    with database.get_connection() as conn:
      conn.execute(create_sql_query)

    """
      Delete existing data in the target table for the partition_date
    """
    delete_sql_query = f"""delete from trips where partition_date = '{partition_date_str}'"""
    with database.get_connection() as conn:
      conn.execute(delete_sql_query)

    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    insert_sql_query = f"""
        insert into trips
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount,
            '{partition_date_str}' as partition_date 
          from 'data/raw/taxi_trips_{month_to_load}.parquet'
        ;
    """

    # conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    # conn.execute(sql_query)      

    with database.get_connection() as conn:
        conn.execute(insert_sql_query)


@asset(
    deps=["taxi_zones_file"],
    group_name = "ingested"
)
def taxi_zones(database: DuckDBResource) -> None:
    sql_query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    # conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    # conn.execute(sql_query)   

    with database.get_connection() as conn:
        conn.execute(sql_query) 