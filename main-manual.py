from json import load
from os import path, environ
from datetime import datetime
from cloudteam_logger import cloudteam_logger
from collectors.collector_inventory import inventory_collector
from collectors.collector_metrics import metrics_collector
from collectors.collector_sizing import sizing_collector
from utils.utils import create_folder_if_not_exist
from boto3 import Session

def main():
    """
    The main function reads account information from a JSON file, creates AWS sessions for each account,
    and then runs parallel tasks to gather information about each mode.
    """
    # arranging all os configs such as path of file running or create folders if not exist
    mode = environ.get('MODE', 'INVENTORY').upper()
    load_json = None
    AUTOMATION = f'aws-{mode.lower()}-collector'
    main_dir = path.dirname(path.abspath(__file__))
    uploads_dir = f'{main_dir}/uploads'
    logs_dir = f'{main_dir}/logs'
    sso_mode = environ.get('SSO_MODE', 'false').lower() == 'true'
    main_dir = path.dirname(path.abspath(__file__))
    create_folder_if_not_exist([f'{main_dir}/uploads',
                               f'{main_dir}/logs'])
    time_generated = environ.get(
        'TIME_GENERATED_SCRIPT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    threads = int(environ.get('THREADS_NUMBER', 8))
    logger_obj = cloudteam_logger.ct_logging(logs_dir, 'debug')
    
    if not sso_mode:
        with open(f'{main_dir}/files/accounts.json', encoding="UTF-8") as file:
            load_json = load(file)
    else:
        # Get the STS client
        sts = Session().client("sts")

        # Fetch identity info
        identity = sts.get_caller_identity()
        load_json = {'accounts': [{"account_id": identity['Account'], "account_role": "Cloudteam-FinOps"}]}
    if mode == 'INVENTORY':
        inventory_collector(uploads_directory=uploads_dir, logger=logger_obj,
                            accounts_json=load_json, time_generated=time_generated, threads=threads, sso_mode=sso_mode)
    elif mode == 'METRICS':
        with open(f'{main_dir}/files/metrics.json', encoding="UTF-8") as file:
            metrics_list = load(file)
        metrics_collector(uploads_directory=uploads_dir, logger=logger_obj,
                          accounts_json=load_json, time_generated=time_generated, metrics=metrics_list, threads=threads, sso_mode=sso_mode)
    elif mode == 'SIZING':
        sizing_collector(uploads_directory=uploads_dir, logger=logger_obj,
                         accounts_json=load_json, time_generated=time_generated, threads=threads)


if __name__ == '__main__':
    main()
