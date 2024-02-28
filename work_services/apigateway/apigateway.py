import boto3
from utils.utils import *



def list_apigateway(file_path,session,region):
    apigateway_client = session.client('apigateway',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = apigateway_client.get_rest_apis()
    if len(inventory['items']) != 0:
        for i in inventory['items']:
           api_id = i['id']
           arn = f"arn:aws:apigateway:{region}::/apis/{api_id}"
           inventory_api = apigateway_client.get_resources(restApiId=api_id)['items']
           inventory_object = extract_common_info(arn,inventory_api,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances