import boto3
from utils.utils import *

def list_elbv2(file_path,session,region):
    client = session.client('elbv2',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    elb_list = client.describe_load_balancers()
    resources = []
    if len(elb_list['LoadBalancers']) != 0:
        for i in elb_list['LoadBalancers']:
            arn = i['LoadBalancerArn']
            resouce_object = extract_common_info(arn,i,region,account_id)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources