
from asyncio import as_completed
from http import client
import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from collector.collector_inventory import *
# from work_services import *


# change to reginal and non reginal
def main():
    """
    The main function reads account information from a JSON file, creates AWS sessions for each account,
    and then runs parallel tasks to gather inventory and list S3 buckets for each account.
    """
    
    main_dir = os.path.dirname(os.path.abspath(__file__))
    uploads = main_dir + '/uploads'
    f = open(f'{main_dir}/files/account.json')
    load_json = json.load(f)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(load_json['accounts'])) as executor:
        futures_services = {
            executor.submit(
                lambda acc=account, reg=region: parallel_executor_inventory(
                    uploads, get_aws_session(acc['account_id'], reg), reg
                ), account, region  # Defaulting acc and reg inside lambda
            ): account for account in load_json['accounts'] for region in regions_enabled(get_aws_session(account['account_id']))
        }
        for future in concurrent.futures.as_completed(futures_services):
            try:
                result = future.result()  # Corrected to call .result() on future
                # Process result or print
            except Exception as ex:
                print(ex)
    

    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(load_json['accounts'])) as executor:
    #     futures = []
    #     for account in load_json['accounts']:
    #         future = executor.submit(
    #             lambda acc=account: list_s3_buckets(uploads,get_aws_session(acc['account_id']))
    #         )
    #         futures.append(future)
    #     for fu in concurrent.futures.as_completed(futures):
    #         try:
    #             result = future.result()
    #             # print(result)
    #         except Exception as ex:
    #             print(ex)
                

    

    # readfile = f'{main_dir}/mama.json'
    # with open(f'{readfile}','r',encoding='utf-8',errors='ignore') as test_file:
    #     test = test_file.read()
    #     test = json.loads(test)
    #     prop = eval(test['properties'].replace('datetime.datetime', 'datetime').replace('tzutc()', 'pytz.utc'))
    #     for item in prop:
    #         item['Timestamp'] = item['Timestamp'].isoformat()
    #         print(item)
    # load_json_test = json.load(test)

    # for account in load_json['accounts']:
    #     session = get_aws_session(account['account_id'])
    #     regions = regions_enabled(session)
    #     for region in regions:
    #         session = get_aws_session(account['account_id'],region)
    #         parallel_executor_inventory(uploads,session,region)
                
    # for account in load_json['accounts']:
    #     session = get_aws_session(account['account_id'])
    #     list_s3_buckets(uploads,session)
        # parallel_executor_inventory_s3(uploads,session)
    # get_aws_session()x
    # check if insert session into region will solve the incorrect tokenId
    # get_accounts_in_org()
  
    
    # print(sqs)
   
if __name__ == '__main__':
    main()
