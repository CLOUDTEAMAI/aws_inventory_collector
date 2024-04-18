import boto3
from utils.utils import *




def list_cloudformation(file_path,session,region,time_generated,account):
    client = session.client('cloudformation',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    cloud_list = client.describe_stacks()
    resources = []
    if len(cloud_list['Stacks']) != 0:
        for i in cloud_list['Stacks']:

            if 'CreationTime' in i:
                i['CreationTime'] = i['CreationTime'].isoformat()
            if 'DeletionTime' in i:
                i['DeletionTime'] = i['DeletionTime'].isoformat()
            if 'LastUpdatedTime' in i:
                i['LastUpdatedTime'] = i['LastUpdatedTime'].isoformat()
            if 'LastCheckTimestamp' in i['DriftInformation']:
                i['DriftInformation']['LastCheckTimestamp'] =i['DriftInformation']['LastCheckTimestamp'].isoformat()
            
            arn = i['StackId']
            resouce_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        # return resources