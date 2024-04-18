import boto3
from utils.utils import *

def list_elbv2(file_path,session,region,time_generated,account):
    client = session.client('elbv2',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    elb_list = client.describe_load_balancers()
    resources = []
    if len(elb_list['LoadBalancers']) != 0:
        for i in elb_list['LoadBalancers']:
            if 'CreatedTime' in i:
                i['CreatedTime'] = i['CreatedTime'].isoformat()
            arn = i['LoadBalancerArn']
            resouce_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        # return resources