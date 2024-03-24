import boto3
import os
from utils.utils import *
def get_accounts_in_org(file_path,session):
    org_collector = []
    org_client = session.client('organizations')
    try:
        accounts = org_client.list_accounts()
        for acc in accounts['Accounts']:
            arn = acc['Arn']
            if 'JoinedTimestamp' in acc:
                acc['JoinedTimestamp'] = acc['JoinedTimestamp'].isoformat()
            client_object = extract_common_info(arn,acc,None,acc['Id'])
            org_collector.append(client_object)
        save_as_file_parquet(org_collector,file_path,generate_parquet_prefix(__file__,'global',acc['Id']))

            

        return accounts
    except Exception as ex:
        print(ex)
    # print(accounts['Accounts'])