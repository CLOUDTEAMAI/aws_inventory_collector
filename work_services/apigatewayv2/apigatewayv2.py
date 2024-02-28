import boto3
from utils.utils import *



def list_apigatewayv2(file_path,session,region):
    apigatewayv2_client = session.client('apigatewayv2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = apigatewayv2_client.get_apis()
    if len(inventory['Items']) != 0:
        for i in inventory['Items']:
           api_id = i['ApiId']
           arn = f"arn:aws:apigateway:{region}::/apis/{api_id}"
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances