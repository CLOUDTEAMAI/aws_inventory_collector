import boto3
from utils.utils import *


def list_accessanalyzer(file_path,session,region,time_generated,account):
    client = session.client('accessanalyzer',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    client_list = client.list_analyzers()
    resources = []
    if len(client_list['analyzers']) != 0:
        for i in client_list['analyzers']:
            if 'lastResourceAnalyzedAt' in i:
                i['lastResourceAnalyzedAt'] = i['lastResourceAnalyzedAt'].isoformat()
            if 'createdAt' in i:
                i['createdAt'] = i['createdAt'].isoformat()
            arn = i['arn']
            resouce_object = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources