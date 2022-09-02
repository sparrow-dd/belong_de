import pandas as pd
import load_parquet

def create_date_table (start = '2000-01-01', end = '2100-01-01'):
    df = pd.DataFrame({"date": pd.date_range(start,end)})
    df["day"] = df["date"].dt.day_name()
    df["week"] = df['date'].dt.weekofyear
    df['month'] = df['date'].dt.month
    df["quarter"] = df["date"].dt.quarter
    df["year"] = df["date"].dt.year
    return df

if __name__ == '__main__':
    glue_db = "pedestrian-counting-system"

    glue_table = "calendar-date-table"

    dtype = {    'date':'date',
                'day':'string',
                'week':'int',
                'month':'int',
                'quarter':'int',
                'year':'int',
    }

    date_table = create_date_table()   
    print(date_table) 
    athena_folder = "s3://athena-bucket-data-table/"
    load_parquet.convert_to_parquet_s3(df=date_table, location=athena_folder, glue_db=glue_db, glue_table=glue_table, columns_types_dict=None,dtype_dict=None,partition_cols=None,partition_types_dict=None)

