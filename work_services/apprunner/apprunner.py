import boto3
from utils.utils import *

def list_apprunner(file_path,session,region,time_generated,account):
    client_boto = session.client('apprunner',region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ","_")
    apprunner_list_boto3 = client_boto.list_services()
    apprunner_list = []
    region          = region
    if len(apprunner_list_boto3['ServiceSummaryList']) != 0:
        for i in apprunner_list_boto3['ServiceSummaryList']:
            arn = i['ServiceArn']
            object_client = extract_common_info(arn,i,region,account_id,time_generated,account_name)
            apprunner_list.append(object_client)
        save_as_file_parquet(apprunner_list,file_path,generate_parquet_prefix(__file__,region,account_id))
    