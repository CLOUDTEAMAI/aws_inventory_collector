from time import sleep
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
    create_folder_if_not_exist([f'{main_dir}/uploads',f'{main_dir}/files',f'{main_dir}/logs',f'{main_dir}/uploads/metrics',f'{main_dir}/query_data'])
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
        # logger_obj.error(str(ex))
        print(f"faild to write logger s3 {ex}")

    get_all_accounts_metrics(main_dir=f"{uploads}/metrics",logger_obj=logger_obj,account_json=load_json)
    
    # Finsihed Collecting All the Data


    start_timer = datetime.now()
    
    if os.environ.get('MANUAL_INSERT_TO_DB'):
        db_manager = DatabaseManager()
        
        if os.environ.get('TABLE_NAME'):
            db_manager.create_table_collector(os.environ.get('TABLE_NAME'))
            data_list = db_manager.load_data_from_dir_parquet(uploads)
            db_manager.insert_data_collector(os.environ.get('TABLE_NAME'), data_list, update_on_conflict=True)
        
        if  os.environ.get('TABLE_NAME_METRIC'):
            db_manager.create_table_metric(os.environ.get('TABLE_NAME_METRIC'))
            data_list_metric = db_manager.load_data_from_dir_parquet(f"{uploads}/metrics")
            db_manager.insert_data_metric(os.environ.get('TABLE_NAME_METRIC'), data_list_metric, update_on_conflict=True)
        
        if os.environ.get('TABLE_NAME_METRIC') and os.environ.get('TABLE_NAME'):
            sleep(5)
            query_result = f"{main_dir}/query_data"
            db_manager.generate_data_from_all_queries(os.environ.get('TABLE_NAME'),query_result,os.environ.get('TABLE_NAME_METRIC'))
            db_manager.close_connection()


    stop_timer = datetime.now()
    runtime = ((stop_timer - start_timer).total_seconds())/60
    print('---------------------------------------')
    print(f"{runtime}")




if __name__ == '__main__':
    main()




