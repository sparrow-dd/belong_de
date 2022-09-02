### convert dataframe into partitioned parquet files on s3
### logging in logs folder
### authentication variables stored in aws_auth.py (not available in github)

import awswrangler as wr
import boto3
import aws_auth
from logger import logger

def convert_to_parquet_s3(df, location, glue_db, glue_table, dtype_dict,columns_types_dict=None,partition_cols=None,partition_types_dict = None):

    logger.info(f"Start converting dataframe to parquet files in S3.")
    
    session = boto3.Session(
        aws_access_key_id=aws_auth.aws_access_key_id,
        aws_secret_access_key=aws_auth.aws_secret_access_key,
        region_name = aws_auth.region_name
    ) 
    try:   
        wr.catalog.create_parquet_table(
        database = glue_db, # Glue/Athena catalog: Database name,
        table= glue_table, # Glue/Athena catalog: Table name,
        path = location,
        columns_types = columns_types_dict,
        partitions_types = partition_types_dict,
        boto3_session=session,
        )
    except AttributeError as e:
        logger.error(f"ERROR:{e}")

    wr.s3.to_parquet(
        df=df,
        path = location,
        dataset=True,
        boto3_session=session,
        database = glue_db, # Glue/Athena catalog: Database name
        table = glue_table, # Glue/Athena catalog: Table name
        dtype = dtype_dict,  # Dictionary of columns names and Athena/Glue types to be casted
        partition_cols = partition_cols,
    )

    logger.info(f"Finish converting dataframe ({len(df)} rows) to parquet files in S3 folder {location}, glue table {glue_table} in glue db {glue_db}.")

def raw_convert_to_parquet_s3(df, location):

    logger.info(f"Start converting dataframe to parquet files in S3.")
    
    session = boto3.Session(
        aws_access_key_id=aws_auth.aws_access_key_id,
        aws_secret_access_key=aws_auth.aws_secret_access_key,
        region_name = aws_auth.region_name
    ) 

    wr.s3.to_parquet(
        df=df,
        path = location,
        dataset=True,
        boto3_session=session,
    )

    logger.info(f"Finish converting dataframe ({len(df)} rows) to parquet files in S3 folder {location}.")
