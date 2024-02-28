import boto3
from utils.utils import *

def list_apprunner(file_path,session,region):
    client_boto = session.client('apprunner',region_name=region)
    sts = session.client('sts')
    account_id = sts.get_caller_identity()["Account"]
    apprunner_list_boto3 = client_boto.list_services()
    apprunner_list = []
    region          = region
    if len(apprunner_list_boto3['ServiceSummaryList']) != 0:
        for i in apprunner_list_boto3['ServiceSummaryList']:
            arn = i['ServiceArn']
            object_client = extract_common_info(arn,i,region,account_id)
            apprunner_list.append(object_client)
        save_as_file_parquet(apprunner_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    