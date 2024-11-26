from dagster import asset,AssetExecutionContext
from . import constants
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
# import duckdb
# import os
from dagster_duckdb import DuckDBResource
import pandas as pd
from ..partitions import weekly_partition

@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    # conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()    

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@asset(
    deps=["manhattan_stats"]
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        labels={'num_trips': 'Number of Trips'}
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)

@asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition
)
def trips_by_week(context:AssetExecutionContext,database: DuckDBResource) -> None:
    # get the distinct start_date and end_date for the periods
    # conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    period_to_fetch = context.partition_key
    
    periods_query = """
                select distinct cast(pickup_datetime as date) as start_date, date_add(cast(pickup_datetime as date), INTERVAL 7 DAYS) as end_date  
                from trips 
                where dayofweek(pickup_datetime)=0 
                order by 1
            """
    with database.get_connection() as conn:
        periods = conn.execute(periods_query).fetch_df()    

    # Create and empty dataframe with the header fields and write to the csv file. The mode to open the csv is 'w' which means it will drop and recreate the file, if it already exists.
    cols =['period','num_trips','passenger_count','total_amount','trip_distance']
    df_header = pd.DataFrame(columns=cols)
    df_header.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, encoding='utf-8',index=False, mode='w')
    
    # For each weekly period (based on the periods derived earlier), query the data, aggregate and wrie to the csv file in append mode ('a'). Make sure the header is set to False during append.  
    for index, row in periods.iterrows():
        agg_query = f"""
                select '{row['start_date']}' as period 
                        ,count(*) as num_trips
                        ,round(sum(passenger_count),2) as passenger_count 
                        ,round(sum(total_amount),2) as total_amount 
                        ,round(sum(trip_distance),2) as trip_distance 
                from trips 
                where pickup_datetime between '{row['start_date']}' and '{row['end_date']}'  
                group by '{row['start_date']}'
        """
        with database.get_connection() as conn:
            trip_by_week = conn.execute(agg_query).fetch_df()
        trip_by_week.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, encoding='utf-8',index=False, mode='a', header=False)
            