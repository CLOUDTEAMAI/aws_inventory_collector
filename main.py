import aioboto3
import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
import os 
from cloudwatch_logic import *
from collector.collector_inventory import *
# from collector.aws_collector_inventory import *
from cloudteam_logger import cloudteam_logger
import threading
from db.database_manager import DatabaseManager



lock = threading.Lock()
def create_folder_if_not_exist(list_dir_path):
    for i in list_dir_path:
        if not os.path.exists(i):
            os.mkdir(i)

def main():
    """
    The main function reads account information from a JSON file, creates AWS sessions for each account,
    and then runs parallel tasks to gather inventory and list S3 buckets for each account.
    """



    #  arranging all os configs such as path of file runing or create folders if not exist
    main_dir = os.path.dirname(os.path.abspath(__file__))
    create_folder_if_not_exist([f'{main_dir}/uploads',f'{main_dir}/files',f'{main_dir}/logs',f'{main_dir}/uploads/metrics'])
    uploads = f'{main_dir}/uploads'
    time_generated = os.environ.get('TIME_GENERATED_SCRIPT')
    if time_generated == None:
        time_generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger_obj = cloudteam_logger.ct_logging(f'{main_dir}/logs','debug')
    f = open(f'{main_dir}/files/account.json')
    load_json = json.load(f)

    # Start collecting all the data from client environment
    start_timer = datetime.now()
    try:
        get_all_accounts_inventory(main_dir=uploads,logger_obj=logger_obj,account_json=load_json,time_generated=time_generated)
        print("Finished Collecting")
    except Exception as ex:
        print(f"faild to execute get_all_accounts_inventory \n{ex}")
    start_timer = datetime.now()
    stop_timer = datetime.now()
    runtime = ((stop_timer - start_timer).total_seconds())/60
    print('---------------------------------------')
    print(f"{runtime}")
    try:
        get_all_accounts_s3(main_dir=uploads,account_json=load_json,logger_obj=logger_obj,time_generated=time_generated)
        print("Finished Collecting S3")
    except Exception as ex:
        logger_obj.error(str(ex))
        print(f"faild to write logger s3 {ex}")

    get_all_accounts_metrics(main_dir=f"{uploads}/metrics",logger_obj=logger_obj,account_json=load_json)
    
    # Finsihed Collecting All the Data


    start_timer = datetime.now()
    
    if os.environ.get('MANUAL_INSERT_TO_DB') and os.environ.get('TABLE_NAME'):
        db_manager = DatabaseManager()
        db_manager.create_table_collector(os.environ.get('TABLE_NAME'))
        data_list = db_manager.load_data_from_dir_parquet(uploads)
        db_manager.insert_data_collector(os.environ.get('TABLE_NAME'), data_list, update_on_conflict=True)
        query_result = f"{main_dir}/query_data"
        db_manager.generate_data_from_all_queries(os.environ.get('TABLE_NAME'),query_result,os.environ.get('TABLE_NAME_METRIC'))
        db_manager.close_connection()


    stop_timer = datetime.now()
    runtime = ((stop_timer - start_timer).total_seconds())/60
    print('---------------------------------------')
    print(f"{runtime}")




if __name__ == '__main__':
    main()





#     # try:
#     #     max_worker = len(load_json['accounts'])
#     #     if max_worker <= 1 or max_worker > 11:
#     #         max_worker = 10
#     #     with concurrent.futures.ThreadPoolExecutor(max_workers=max_worker) as executor:
#     #         futures_services = {
#     #             executor.submit(
#     #                 lambda acc=account, reg=region: parallel_executor_inventory(
#     #                     logger_obj,uploads, get_aws_session(acc['account_id'], reg), reg
#     #                 ), account, region  # Defaulting acc and reg inside lambda
#     #             ): account for account in load_json['accounts'] for region in regions_enabled(get_aws_session(account['account_id']))
#     #         }
#     #         for future in concurrent.futures.as_completed(futures_services):
#     #             try:
#     #                 result = future.result()  # Corrected to call .result() on future
#     #                 # Process result or print
#     #             except Exception as ex:
#     #                 print(f'Error raisd in  \n {ex}')
#     #                 with lock:
#     #                     logger_obj.error(ex)
                    
#     # except Exception as ex:
#     #     print(f'Error in end {ex}')

#     # with concurrent.futures.ThreadPoolExecutor(max_workers=len(load_json['accounts'])) as executor:
#     #     futures = []
#     #     for account in load_json['accounts']:
#     #         future = executor.submit(
#     #             lambda acc=account: list_s3_buckets(uploads,get_aws_session(acc['account_id']))
#     #         )
#     #         futures.append(future)
#     #     for fu in concurrent.futures.as_completed(futures):
#     #         try:
#     #             result = future.result()
#     #             # print(result)
#     #         except Exception as ex:
#     #             print(ex)
#     #             with lock:
#     #                 logger_obj.error(ex)
                

    
#     # files_uploads_read = [
#     # f'{uploads}/ec2-eu-west-1-597320742842-metrics.parquet',f'{uploads}/ec2-eu-west-1-012772511166-metrics.parquet',
#     # f'{uploads}/ec2-eu-west-1-106884039378-metrics.parquet',f'{uploads}/ec2-eu-west-1-561825688699-metrics.parquet',
#     # f'{uploads}/ec2-eu-west-1-145173617099-metrics.parquet',f'{uploads}/ec2-eu-west-1-596850130446-metrics.parquet',
#     # f'{uploads}/ec2-eu-west-1-830533212225-metrics.parquet',f'{uploads}/ec2-eu-west-1-858255827095-metrics.parquet',
#     # f'{uploads}/ec2-eu-west-1-861696922348-metrics.parquet',f'{uploads}/ec2-eu-west-1-862992236389-metrics.parquet'
#     # ]
#     # end_prefix = '.parquet'
#     # for name in files_uploads_read:
#     #     output1_avg = name.split('.')[0]
#     #     prefix = f'{output1_avg}-avg{end_prefix}'
#     #     prefix2 = f'{output1_avg}-max{end_prefix}'
#     #     calculate_statistics(name,prefix,prefix2)
                

                
#     # readfile = f'{main_dir}/mama.json'
#     # with open(f'{readfile}','r',encoding='utf-8',errors='ignore') as test_file:
#     #     test = test_file.read()
#     #     test = json.loads(test)
#     #     prop = eval(test['properties'].replace('datetime.datetime', 'datetime').replace('tzutc()', 'pytz.utc'))
#     #     for item in prop:
#     #         item['Timestamp'] = item['Timestamp'].isoformat()
#     #         print(item)
#     # load_json_test = json.load(test)

#     # for account in load_json['accounts']:
#     #     session = get_aws_session(account['account_id'])
#     #     regions = regions_enabled(session)
#     #     for region in regions:
#     #         session = get_aws_session(account['account_id'],region)
#     #         parallel_executor_inventory(uploads,session,region)
                
#     # for account in load_json['accounts']:
#     #     session = get_aws_session(account['account_id'])
#     #     list_s3_buckets(uploads,session)
#     #     parallel_executor_inventory_s3(uploads,session)
#     # get_aws_session()x
#     # # check if insert session into region will solve the incorrect tokenId
#     # get_accounts_in_org()
  
    
   


# def main1():
#     main_dir = os.path.dirname(os.path.abspath(__file__))
#     # parent_dir = os.path.dirname(main_dir)
#     uploads = main_dir + '/uploads'
#     f = open(f'{main_dir}/files/account.json')
#     load_json = json.load(f)
#     logger_obj = cloudteam_logger.ct_logging(f'{main_dir}/logs','debug')
#     item = AWSInventoryCollector(uploads,logger_obj)
#     item.get_all_accounts_metrics(main_dir=uploads,account_json=load_json)

# async def main():
#     main_dir = os.path.dirname(os.path.abspath(__file__))
#     # parent_dir = os.path.dirname(main_dir)
#     uploads = main_dir + '/uploads'
#     f = open(f'{main_dir}/files/account.json')
#     load_json = json.load(f)
#     logger_obj = cloudteam_logger.ct_logging(f'{main_dir}/logs','debug')
#     item = AWSInventoryCollector(uploads,logger_obj)
#     item.get_all_accounts_metrics(main_dir=uploads,account_json=load_json)
#     # await item.async_get_all_accounts_inventory(load_json)


# if __name__ == "__main__":
#     main1()
    # asyncio.run(main())
