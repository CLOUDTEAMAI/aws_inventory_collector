from json import load
from os import path, environ
from datetime import datetime
from cloudteam_logger import cloudteam_logger
from collectors.collector_inventory import inventory_collector
from collectors.collector_metrics import metrics_collector
from collectors.collector_sizing import sizing_collector
from utils.utils import create_folder_if_not_exist


def main():
    """
    The main function reads account information from a JSON file, creates AWS sessions for each account,
    and then runs parallel tasks to gather information about each mode.
    """
    # arranging all os configs such as path of file runing or create folders if not exist
    mode = environ.get('MODE', 'INVENTORY').upper()
    AUTOMATION = f'aws-{mode.lower()}-collector'
    # try:
    #     with open('/app/secrets/account.json', encoding="UTF-8") as file:
    #         account_json = load(file)
    #     CUSTOMER_ID = str(account_json['CUSTOMER_ID'])
    #     TENANT_ID = account_json['TENANT_ID']
    #     CLIENT_ID = account_json['CLIENT_ID']
    #     CLIENT_SECRET = base64.b64decode(account_json.get(
    #         'CLIENT_SECRET', '').encode("ascii")).decode("ascii")
    #     CERTIFICATE_FILE_PATH = base64.b64decode(account_json.get(
    #         'CERTIFICATE_FILE_PATH', '').encode("ascii")).decode("ascii")
    # except Exception as e:
    #     print("Error loading account.json")
    # QUEUE_URI = environ.get('QUEUE_URI', '')
    # QUEUE_IDENTITY_ID = environ.get('QUEUE_IDENTITY_ID', '')
    # QUEUE_DESTINATION = environ.get("QUEUE_DESTINATION", "posti")
    # log = ""
    # today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # today_file = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # currentdate = datetime.now().strftime("%Y-%m-%d")
    # main_dir = path.dirname(path.abspath(__file__))
    # uploads_dir = f'{main_dir}/uploads/{AUTOMATION}/{CUSTOMER_ID}/{today_file}'
    # logs_dir = f'{main_dir}/logs/{AUTOMATION}/{CUSTOMER_ID}/{today_file}'
    main_dir = path.dirname(path.abspath(__file__))
    uploads_dir = f'{main_dir}/uploads'
    logs_dir = f'{main_dir}/logs'

    main_dir = path.dirname(path.abspath(__file__))
    create_folder_if_not_exist([f'{main_dir}/uploads',
                               f'{main_dir}/logs'])
    time_generated = environ.get(
        'TIME_GENERATED_SCRIPT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    threads = int(environ.get('THREADS_NUMBER', 6))
    logger_obj = cloudteam_logger.ct_logging(logs_dir, 'debug')

    with open(f'{main_dir}/files/accounts.json', encoding="UTF-8") as file:
        load_json = load(file)

    if mode == 'INVENTORY':
        inventory_collector(uploads_directory=uploads_dir, logger=logger_obj,
                            accounts_json=load_json, time_generated=time_generated, threads=threads)
    elif mode == 'METRICS':
        with open(f'{main_dir}/files/metrics.json', encoding="UTF-8") as file:
            metrics_list = load(file)
        metrics_collector(uploads_directory=uploads_dir, logger=logger_obj,
                          accounts_json=load_json, time_generated=time_generated, metrics=metrics_list, threads=threads)
    elif mode == 'SIZING':
        sizing_collector(uploads_directory=uploads_dir, logger=logger_obj,
                         accounts_json=load_json, time_generated=time_generated, threads=threads)


if __name__ == '__main__':
    main()
