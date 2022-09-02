# overall process of accessing, transforming and loading data into S3
import process_raw_data
import load_parquet

# set up folders

mc_raw_folder = "s3://raw-data-pedestrian-system/raw-monthly-counts/"
sl_raw_folder = "s3://raw-data-pedestrian-system/raw-sensor-locations/"

mc_processed_folder = "s3://processed-data-pedestrian-system/processed-monthly-counts/"
sl_processed_folder = "s3://processed-data-pedestrian-system/processed-sensor-locations/"

# set up glue db, table, schema
glue_db = "pedestrian-counting-system-sy"


processed_sl_glue_table = "processed-sensor-locations"
# raw_sl_glue_table = "raw-sensor-locations-table"

sl_dtype = {    'sensor_id':'int',
                'sensor_description':'string',
                'sensor_name':'string',
                'installation_date':'timestamp',
                'status':'string',
                'direction_1':'string',
                'direction_2':'string',
                'latitude':'double',
                'longitude':'double',
                'location':'string',
                'note':'string'
    }

processed_mc_glue_table = "processed-monthly-counts"
# raw_mc_glue_table = "raw-monthly-counts-table"

mc_dtype = {    'id':'int',
                'date_time':'timestamp',
                'year':'int',
                'month':'string',
                'mdate':'int',
                'day':'string',
                'time':'int',
                'sensor_id':'int',
                'sensor_name':'string',
                'hourly_counts':'int'
    }
mc_columns_types_dict =  {    'id':'int',
                'date_time':'timestamp',
                'day':'string',
                'time':'int',
                'sensor_id':'int',
                'sensor_name':'string',
                'hourly_counts':'int'
    }   
mc_partitions_keys = ['year','month','mdate']
mc_partition_types_dict = {'year':'int','month':'string', 'mdate':'int'}

# retrieve data via API and process data
print("Retrieving and processing data")

processed_mc_df,raw_mc_df = process_raw_data.process_monthly_counts()
processed_sl_df,raw_sl_df = process_raw_data.process_sensor_locations()

print("Finish retrieving and processing data")

# save parquet files on local path - backup purpose
print("Loading parquet files on local machine.")
processed_mc_df.to_parquet('processed_mc_df.parquet')
raw_mc_df.to_parquet('raw_mc_df.parquet')
processed_sl_df.to_parquet('processed_sl_df.parquet')
raw_sl_df.to_parquet('raw_sl_df.parquet')
print("Finish loading parquet files on local machine.")

# load raw parquet files into S3 with partitions and glue table schema

print("Loading raw parquet files into s3.")
load_parquet.raw_convert_to_parquet_s3(df=raw_mc_df, location=mc_raw_folder)
load_parquet.raw_convert_to_parquet_s3(df=raw_sl_df, location=sl_raw_folder)
print("Finish loading raw parquet files into s3.")

print("Loading processed parquet files into s3 for monthly counts data.")
# load processed parquet files into S3 with partitions and glue table schema
# load_parquet.convert_to_parquet_s3(df=processed_mc_df, location=mc_processed_folder, glue_db=glue_db, glue_table=processed_mc_glue_table, columns_types_dict=mc_columns_types_dict,dtype_dict=mc_dtype,partition_cols=None,partition_types_dict=None)
load_parquet.convert_to_parquet_s3(df=processed_mc_df, location=mc_processed_folder, glue_db=glue_db, glue_table=processed_mc_glue_table, columns_types_dict=mc_columns_types_dict,dtype_dict=mc_dtype,partition_cols=mc_partitions_keys,partition_types_dict=mc_partition_types_dict)

print("Loading processed parquet files into s3 for sensor locations data.")
load_parquet.convert_to_parquet_s3(df=processed_sl_df, location=sl_processed_folder, glue_db=glue_db, glue_table=processed_sl_glue_table, columns_types_dict=sl_dtype ,dtype_dict=sl_dtype,partition_cols=None,partition_types_dict=None)

print("Finish loading processed parquet files into s3.")