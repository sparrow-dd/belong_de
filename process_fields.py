### processing fields based on target data types

import pandas as pd
from datetime import datetime
from logger import logger

def str_to_dt(str):
    """
    Convert string to datetime object.        
    Observed formats of datetime string is one of the below:
    YYYY-MM-DDTHH:MM:SS:000
    YYYY-MM-DD HH:MM:SS        
    DD/MM/YYYY HH:MM:SS
    :param str: input date time str
    :return date_time object    
    """    
    str_formats = ["%Y-%m-%d %H:%M:%S","%d/%m/%Y %H:%M:%S","%Y-%m-%dT%H:%M:%S.%f"]
    # try to convert from one of the formats above
    for str_format in str_formats:        
        try: 
            dt_obj = datetime.strptime(str,str_format)            
            return dt_obj
        except:
            continue
    raise ValueError('no valid date format found.')

def process_column(df,column, data_type):
    """Validate and transform fields.
    :param column: column name to be processed
    :param data_type: data type to be converted to (str,int,float,datetime)
    :return int, int (number of values processed)
    """

    # validate and transform fields
    logger.info(f"Start processing field {column} to {data_type}.")

    logger.info(f"Cleaning up values in {column}.")

    count_convert = 0   
    count_invalid_conversion = 0 

    # iterate row in df
    for i, row in df.iterrows():
        # check value 
        raw_str = df.at[i,column]  

        # when not None convert to datetime
        
        if pd.isna(raw_str) == False:                          
            # process datetime fields
            if data_type == 'datetime': 
                try:
                    processed_data = str_to_dt(raw_str)
                    df.at[i,column] = processed_data
                    count_convert +=1                 
                # default to 1900-01-01 with invalid date time format and log errors    
                except:
                    count_invalid_conversion +=1
                    df.at[i,column] = str_to_dt("1900-01-01T00:00:00.000")
                    logger.exception(f"Couldn't parse {raw_str} to {data_type} at row index {i}.")
            
            # process int fields
            elif data_type == 'int':       
                try:
                    processed_data = int(raw_str)
                    df.at[i,column] = processed_data
                    count_convert +=1
                
                # default to 0 with invalid values and log errors    
                except:
                    count_invalid_conversion +=1
                    df.at[i,column] = 0
                    logger.exception(f"Couldn't parse {raw_str} to {data_type} at row index {i}.")    
            
            # process float fields
            elif data_type == 'float':       
                try:
                    processed_data = float(raw_str)
                    df.at[i,column] = processed_data
                    count_convert +=1
                
                # default to 0 with invalid values and log errors    
                except:
                    count_invalid_conversion +=1
                    df.at[i,column] = 0
                    logger.exception(f"Couldn't parse {raw_str} to {data_type} at row index {i}.")   

            # process str fields
            elif data_type == 'str':       
                try:
                    processed_data = str(raw_str).upper()
                    df.at[i,column] = processed_data
                    count_convert +=1
                
                # default to '' with invalid values and log errors    
                except:
                    count_invalid_conversion +=1
                    df.at[i,column] = ''
                    logger.exception(f"Couldn't parse {raw_str} to {data_type} at row index {i}.")  


    logger.info(f"Finish cleaning up values in {column}.")

    # summary 
    logger.info(f"Finish processing {column} : converted {count_convert} values; removed/converted {count_invalid_conversion} invalid values.")

    return  count_convert, count_invalid_conversion   