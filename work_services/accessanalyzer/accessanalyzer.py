import boto3
from utils.utils import *


def list_accessanalyzer(file_path,session,region):
    sts = session.client('sts')
    client = session.client('accessanalyzer',region_name=region)
    account_id = sts.get_caller_identity()["Account"]
    client_list = client.list_analyzers()
    resources = []
    if len(client_list['analyzers']) != 0:
        for i in client_list['analyzers']:
            if 'lastResourceAnalyzedAt' in i:
                i['lastResourceAnalyzedAt'] = i['lastResourceAnalyzedAt'].isoformat()
            if 'createdAt' in i:
                i['createdAt'] = i['createdAt'].isoformat()
            arn = i['arn']
            resouce_object = extract_common_info(arn,i,region,account_id)
            resources.append(resouce_object)
        save_as_file_parquet(resources,file_path,generate_parquet_prefix(__file__,region,account_id))
        return resources