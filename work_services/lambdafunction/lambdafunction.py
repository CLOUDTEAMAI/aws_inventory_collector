import boto3
import pandas as pd
from datetime import datetime
from utils.utils import *




def list_lambda(file_path,session,region,time_generated,account):
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    lambda_client = session.client('lambda',region_name=region)
    lambda_list = lambda_client.list_functions()['Functions']
    resources = []
    for i in lambda_list:
        if 'ApplyOn' in i['SnapStart'] and i['SnapStart']['ApplyOn'] == "None":
            i['SnapStart']['ApplyOn'] = "null"
        arn = i.get('FunctionArn', 'Unknown ARN')
        inventory_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
        resources.append(inventory_object)
    save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
    return resources
