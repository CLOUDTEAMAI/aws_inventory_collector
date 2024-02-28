import boto3
from utils.utils import *



def list_route53(file_path,session,region):
    route53_client = session.client('route53',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = route53_client.list_hosted_zones()
    if len(inventory['HostedZones']) != 0:
        for i in inventory['HostedZones']:
           arn = f"arn:aws:route53::{':'.join(i['Id'].split('/'))}"
           inventory_object = extract_common_info(arn,i,region,account_id)
           inventory_instances.append(inventory_object)
        save_as_file_parquet(inventory_instances,file_path,generate_parquet_prefix(__file__,region,account_id))
    return inventory_instances