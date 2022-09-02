### Access source data via API and convert into dataframe
### logging in logs folder
### data overview in reports folder
### authentication variables stored in api_auth.py (not available in github)

import pandas as pd
from sodapy import Socrata
import api_auth
from logger import logger,report
from datetime import datetime

# get current date time
now = datetime.now()

now_str = now.strftime("%Y%m%d%H%M5S")
current_year = now.strftime("%Y")

# Authenticated client (variables retrieved from api_auth.py)
client = Socrata(api_auth.dataset_url,
                 api_auth.MyAppToken,
                 api_auth.username,
                 api_auth.password)

def request_dataset(dataset_id, primary_key, test_mode = False):
    '''
    Make an API call and get data in batches (50000 limitation per request of the source data API)
    results returned as JSON from API and converted to pandas dataframe
    :param dataset_id: dataset id str
    :param primary_key: primay key name in source dataset
    :return: dataframe, datetime
    '''    
    dataset_metadata = [x for x in client.datasets() if x['resource']['id'] == dataset_id]
    dataset_name = dataset_metadata[0]['resource']['name']
    dataset_updated_at = dataset_metadata[0]['resource']['updatedAt']
    
    logger.info(f"Start requesting dataset {dataset_name} last updated at {dataset_updated_at} from source.")

    # limit_for_test_purpose = 150000
    batch = 50000
    start = 0
    results = pd.DataFrame.from_records(client.get(dataset_id, limit=batch, offset=start, order=primary_key)) 

    # load data in batches
    # stop when the last batch does not have 50000 rows
    
    while results.empty is False and len(results) % batch == 0 and test_mode == False:         
        start = start + batch
        next_batch = pd.DataFrame.from_records(client.get(dataset_id, limit=batch, offset=start, order=primary_key))
        results = pd.concat([results, next_batch], ignore_index=True)
        # print(len(results))      

    logger.info(f"Finish requesting dataset {dataset_name} last updated at {dataset_updated_at} from source. Row count: {len(results)}.")
    
    # write detailed information into report.txt file
    report.write("------------------------------------------------------------\n")
    report.write(f"Start inspecting dataframe: {dataset_name} at {now_str}\n")        
    report.write(f"Total row count: {str(len(results))}\n")        
    report.write(f"Top 5 rows:\n {results.head()}\n")
    report.write(f"Columns with data types:\n{dict(results.dtypes)}\n")
    report.write(f"Check null values for each column:\n{results.isnull().sum()}\n")
    report.write(f"Summary :\n{results.describe()}\n")        
    report.write(f"End of inspecting dataframe: {dataset_name}\n")
    report.write("------------------------------------------------------------\n")

    return results, dataset_updated_at

if __name__ == '__main__':

    monthly_counts_raw, monthly_counts_raw_updated_at = request_dataset(api_auth.monthly_counts_dataset_id, api_auth.monthly_counts_pk)
    sensor_locations_raw, sensor_locations_raw_updated_at = request_dataset(api_auth.sensor_locations_dataset_id,api_auth.sensor_locations_pk)
    print(monthly_counts_raw.head(),monthly_counts_raw_updated_at)
    print(sensor_locations_raw.head(),sensor_locations_raw_updated_at)



