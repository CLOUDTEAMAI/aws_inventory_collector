import boto3
from utils.utils import *


def list_elb(file_path,session,region):
    client = session.client('elb',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    elb_list = client.describe_load_balancers()
    resources = []
    if len(elb_list['LoadBalancerDescriptions']) != 0:
        for i in elb_list['LoadBalancerDescriptions']:

            if 'CreatedTime' in i:
                i['CreatedTime'] = i['CreatedTime'].isoformat()
                
            arn = i['LoadBalancerArn']
            resouce_object = extract_common_info(arn,i,region,account_id)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources
    



'''
save parquet with 100 items at least
'''