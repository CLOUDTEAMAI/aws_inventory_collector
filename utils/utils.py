from datetime import datetime, timedelta
import boto3
import pandas as pd
import json
import os
import ast

def extract_common_info(arn, resource, region, account_id):
    return {
        "arn": arn,
        "account_id": account_id,
        "region": region,
        "properties": resource,
        "timegenerated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def extract_common_info_metrics(id,resource,label):
    return {
        'id': id,
        'label': label,
        'properties': str(resource),
        'timegenerated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    }


def save_as_file_parquet(inventory,file_path,file_name):
    if len(inventory) != 0:
        df = pd.DataFrame(inventory)
        file_path = os.path.join(file_path, file_name)
        # Ensure 'properties' is a string (JSON), as Parquet requires consistent data types
        df['properties'] = df['properties'].apply(lambda x: json.dumps(x) if not isinstance(x, str) else x)   
        # Save the DataFrame to a Parquet file
        df.to_parquet(file_path, index=False)



def get_script_name_without_extension(script_path):
    """
    Extracts the script name without the .py extension from the full path.
    """
    return os.path.splitext(os.path.basename(script_path))[0]


def generate_parquet_prefix(script_path, region, account_id):
    """
    Generates a prefix for a Parquet file including the script name, region, and account_id.
    """
    script_name = get_script_name_without_extension(script_path)
    return f'{script_name}-{region}-{account_id}.parquet'






# def save_as_file_parquet_metric(inventory,file_path,file_name):
#     try:
#         if len(inventory) != 0:
#             df = pd.DataFrame(inventory)
#             file_path = os.path.join(file_path, file_name)
#             # Ensure 'properties' is a string (JSON), as Parquet requires consistent data types
#             df['properties'] = df['properties'].apply(lambda x: json.dumps(x) if not isinstance(x, str) else x)  
#             # Save the DataFrame to a Parquet file
#             df.to_parquet(file_path, index=False)
#     except Exception as ex:
#         print(ex)
