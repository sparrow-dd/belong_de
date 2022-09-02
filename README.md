# belong_de
 
**api_auth.py** - stores authentication tokens for data source (file not in github)

**aws_auth.py** - stores authentication keys for AWS admin account (file not in github)

**logger.py** - logging and writing source data overview reports

**access_data.py** - creates function(s) to retrieve source data via SODA API and convert json into dataframe format, using sodapy library

**access_data_test.py** - test custom function(s) in access_data.py, using unittest library

**process_fields.py** - creates function(s) to cleanse and transform columns based on different target data types, including datetime, int, string, float (double) , using panda library

**process_fields_test.py** - test custom function(s) in process_fields.py, using unittest library

**process_raw_data.py** - creates class and function(s) to validate input dataframe columns, remove duplicates, transform columns and process raw datasets

**load_parquet.py** - creates function (s) to convert dataframe into parquet files with partitions and store raw/processed files in S3 buckets

**ETL.py** - sets variables and run access_data, process_raw_data and load_parquet and generates base layer fact table processed_monthly_counts

**create_calendar_table.py** - generates calendar date dimension table from 2000-01-01 to 2100-01-01

**processed_sensor_locations.sql** - transforms base layer dimension table processed_sensor_locations

**base_perdestrian_monthly_counts.sql** - generates middle layer base_perdestrian_monthly_counts

**agg_perdestrian_monthly_counts_daily.sql** - generates aggregation layer agg_perdestrian_monthly_counts_daily

**agg_perdestrian_monthly_counts_monthly.sql** - generates aggregation layer agg_perdestrian_monthly_counts_monthly

**processed_sl_df.parquet** - local copy of processed_sl_df

**processed_mc_df.parquet** - local copy of processed_mc_df

**raw_sl_df.parquet**  - local copy of raw_sl_df

**raw_mc_df.parquet**  - local copy of raw_mc_df

**AWS Data Architecture.pdf** - Architecture diagram

**ERD.pdf** - data structure design

**partitions in s3.docx** - partition structrure in s3

**pedestrian-counts-insights.twbx** - Tableau workbook

**Tableau Visualisation.docx** - Tableau workbook screenshots 

