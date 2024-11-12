from json import load, dumps
from os import path, environ
from datetime import datetime
from cloudteam_logger import cloudteam_logger
from collectors.collector_inventory import inventory_collector
from collectors.collector_metrics import metrics_collector
from collectors.collector_billing import billing_collector
from collectors.collector_sizing import sizing_collector
from utils.utils import create_folder_if_not_exist
from cloudteam_servicebus import cloudteam_servicebus


def main():
    """
    The main function reads account information from a JSON file, creates AWS sessions for each account,
    and then runs parallel tasks to gather information about each mode.
    """
    # arranging all os configs such as path of file runing or create folders if not exist
    try:
        with open('/app/secrets/account.json', encoding="UTF-8") as file:
            load_json = load(file)
        CUSTOMER_ID = str(load_json['customer_id'])
    except Exception:
        print("Error loading account.json")
    mode = environ.get('MODE', 'METRICS').upper()
    AUTOMATION = f'aws-{mode.lower()}-collector'
    log = ""
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_file = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    main_dir = path.dirname(path.abspath(__file__))
    uploads_dir = f'{main_dir}/uploads/{AUTOMATION}/{CUSTOMER_ID}/{today_file}'
    logs_dir = f'{main_dir}/logs/{AUTOMATION}/{CUSTOMER_ID}/{today_file}'
    time_generated = environ.get(
        'TIME_GENERATED_SCRIPT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    threads = int(environ.get('THREADS_NUMBER', 6))

    QUEUE_URI = environ.get('QUEUE_URI', '')
    QUEUE_IDENTITY_ID = environ.get('QUEUE_IDENTITY_ID', '')
    QUEUE_DESTINATION = environ.get("QUEUE_DESTINATION", "posti")

    create_folder_if_not_exist([f'{main_dir}/uploads/{AUTOMATION}/{CUSTOMER_ID}/{today_file}',
                               f'{main_dir}/logs/{AUTOMATION}/{CUSTOMER_ID}/{today_file}'])
    logger_obj = cloudteam_logger.ct_logging(logs_dir, 'debug')
    try:
        sb = cloudteam_servicebus.ServiceBus(
            QUEUE_URI, QUEUE_IDENTITY_ID, logger_obj)
    except Exception as e:
        logger_obj.error(f"Failed to connect to queue service: {e}")
        exit(1)
    
    if mode == 'INVENTORY':
        inventory_collector(uploads_directory=uploads_dir, logger=logger_obj,
                            accounts_json=load_json, time_generated=time_generated, threads=threads)
    elif mode == 'METRICS':
        with open(f'{main_dir}/files/metrics.json', encoding="UTF-8") as file:
            metrics_list = load(file)
        metrics_collector(uploads_directory=uploads_dir, logger=logger_obj,
                          accounts_json=load_json, time_generated=time_generated, metrics=metrics_list, threads=threads)
    elif mode == 'BILLING':
        billing_collector(uploads_directory=uploads_dir, logger=logger_obj,
                          account=load_json, time_generated=time_generated)
    elif mode == 'SIZING':
        sizing_collector(uploads_directory=uploads_dir, logger=logger_obj,
                         accounts_json=load_json, time_generated=time_generated, threads=threads)
    queue_message = {
        'timegenerated': today,
        'automation': AUTOMATION,
        'customerid': CUSTOMER_ID,
        'uploads_dir': uploads_dir,
        'logs_dir': logs_dir,
    }
    sb.Send_Message(queueName=QUEUE_DESTINATION, message=str(
        dumps(queue_message)).replace("'", '"'))


if __name__ == '__main__':
    main()
