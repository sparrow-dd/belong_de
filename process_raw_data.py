### process raw data

import pandas as pd
import access_data
from datetime import datetime
from datetime import timedelta
from logger import logger
import api_auth
import process_fields


class ValidateTransformRawData:
    """Define a class to validate and transform raw data."""
    # define expected_columns

    def __init__(self, data, expected_columns, dt_columns, int_columns, float_columns, str_columns):
        """Define properties."""
        self.data = data
        self.expected_columns = expected_columns
        self.dt_columns = dt_columns
        self.int_columns = int_columns
        self.float_columns = float_columns
        self.str_columns = str_columns

    def validate_columns(self):
        """Validate columns in raw data."""        

        logger.info(f"Start validating columns in raw data.")

        # check if the columns are the same as expected columns
        if list(self.data.columns) == self.expected_columns:
            logger.info(f"Columns are as expected: {self.expected_columns}")
            logger.info(f"Finish validating columns in raw data.")
            return True  
        else:
            logger.error(f"Columns are {list(self.data.columns)}, not as expected columns {self.expected_columns}.")
            return False
    
    def remove_duplicates(self):
        """Check and remove duplicates."""

        logger.info(f"Start checking duplicates in raw data.")
        
        # check if the dataframe has duplicate rows
        if not self.data.duplicated().empty:            
            logger.info(f"{len(self.data[self.data.duplicated()])} duplicated row(s) have been found.")
            logger.info(f"{len(self.data)} row(s) before dropping duplicates.")

            # drop duplicated rows and only keep one row for each duplicate pair 
            self.data.drop_duplicates(inplace=True, ignore_index=True)
            logger.info(f"{len(self.data)} row(s) after dropping duplicates.")

        else:
            logger.info(f"No duplicated row(s).")

        logger.info(f"Finish checking duplicates in raw data.")

    def process_columns(self):
        '''
        process datetime columns
        '''
        logger.info(f"Start processing columns based on data type.")
        if self.dt_columns is not None:
            for column in self.dt_columns:
                process_fields.process_column(self.data,column,data_type='datetime') 
                self.data = self.data.astype({column: 'datetime64[ns]'})
        if self.int_columns is not None:
            for column in self.int_columns:
                process_fields.process_column(self.data,column,data_type='int')
                self.data = self.data.astype({column: 'int'})
        if self.float_columns is not None:
            for column in self.float_columns:
                process_fields.process_column(self.data,column,data_type='float') 
                self.data = self.data.astype({column: 'float'})
        if self.str_columns is not None:
            for column in self.str_columns:
                process_fields.process_column(self.data,column,data_type='str') 
                self.data = self.data.astype({column: 'str'})   

def process_monthly_counts():
    '''
    process monthly counts raw data 
    
    '''
    # get raw data df
    monthly_counts_raw, monthly_counts_raw_updated_at = access_data.request_dataset(api_auth.monthly_counts_dataset_id, api_auth.monthly_counts_pk)
    
    # set up columns lists
    monthly_counts_columns = ['id', 'date_time', 'year', 'month', 'mdate', 'day', 'time', 'sensor_id', 'sensor_name', 'hourly_counts']

    mc_dt_columns = [ 'date_time']
    mc_str_columns = ['month', 'day', 'sensor_name']
    mc_int_columns = ['id', 'year', 'mdate', 'time', 'sensor_id', 'hourly_counts']

    logger.info("Start processing monthly counts raw data.")
    # initiate validate class
    mc_data = ValidateTransformRawData(data=monthly_counts_raw, expected_columns=monthly_counts_columns, 
                                        dt_columns=mc_dt_columns, int_columns=mc_int_columns,float_columns=None, str_columns=mc_str_columns)
    # process data
    mc_data.validate_columns()
    mc_data.process_columns()
    mc_data.remove_duplicates()

    logger.info("Finish processing monthly counts raw data.") 
    return mc_data.data, monthly_counts_raw   
 
def process_sensor_locations():
    '''
    process sensor locations raw data 
    
    '''
    # get raw data df
    sensor_locations_raw, sensor_locations_raw_updated_at = access_data.request_dataset(api_auth.sensor_locations_dataset_id,api_auth.sensor_locations_pk)
    
    # set up columns lists
    sensor_locations_columns = ['sensor_id', 'sensor_description', 'sensor_name', 'installation_date', 'status', 'direction_1', 'direction_2', 'latitude', 'longitude', 'location', 'note']

    sl_dt_columns = ['installation_date']
    sl_str_columns = ['sensor_description', 'sensor_name', 'status', 'direction_1', 'direction_2', 'location', 'note']
    sl_int_columns = ['sensor_id']
    sl_float_columns = ['latitude', 'longitude'] 

    logger.info("Start processing sensor locations raw data.")
    # initiate validate class
    sl_data = ValidateTransformRawData(data=sensor_locations_raw, expected_columns=sensor_locations_columns, 
                                    dt_columns=sl_dt_columns, int_columns=sl_int_columns, float_columns= sl_float_columns,str_columns=sl_str_columns)
    # process data
    sl_data.validate_columns()    
    sl_data.process_columns()
    sl_data.remove_duplicates()

    logger.info("Finish processing sensor locations raw data.")
    return sl_data.data, sensor_locations_raw

if __name__ == '__main__':
    # mc_df = process_monthly_counts()[0]
    # print(mc_df.dtypes)
    sl_df=process_sensor_locations()[0]
    print(sl_df.dtypes)
    

    




