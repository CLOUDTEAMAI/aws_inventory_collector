import boto3
import pandas as pd
from datetime import datetime
from utils.utils import *




def list_lambda(file_path,session,region):
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    lambda_client = session.client('lambda',region_name=region)
    lambda_list = lambda_client.list_functions()['Functions']
    resources = []
    for i in lambda_list:
        if 'ApplyOn' in i['SnapStart'] and i['SnapStart']['ApplyOn'] == "None":
            i['SnapStart']['ApplyOn'] = "null"
        arn = i.get('FunctionArn', 'Unknown ARN')
        inventory_object = extract_common_info(arn,i,region,account_id)
        resources.append(inventory_object)
    save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
    return resources
