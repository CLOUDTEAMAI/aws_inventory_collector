from json import load, dumps
from os import path, getenv
from datetime import datetime
from cloudteam_servicebus import cloudteam_servicebus
from cloudteam_logger import cloudteam_logger
from collectors.collector_inventory import inventory_collector
from collectors.collector_metrics import metrics_collector
from collectors.collector_billing import billing_collector
from collectors.collector_sizing import sizing_collector
from utils.utils import create_folder_if_not_exist


def main():
    """
    The main function reads account information from a JSON file, creates AWS sessions for each account,
    and then runs parallel tasks to gather information about each mode.
    """
    # arranging all os configs such as path of file runing or create folders if not exist
    mode = getenv('MODE', '').upper()
    AUTOMATION = f'aws-{mode.lower()}-collector'
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_file = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    DATE_YEAR_MONTH = datetime.now().strftime("%Y-%m")
    main_dir = path.dirname(path.abspath(__file__))
    ACCOUNT_FILE_PATH = getenv(
        "ACCOUNT_FILE_PATH", "/app/secrets/data.json")
    queue_client = None
    CUSTOMER_ID = ""
    CUSTOMER_NAME = ""
    uploads_dir = ""
    json_data = {}
    try:
        with open(ACCOUNT_FILE_PATH, encoding="UTF-8") as file:
            load_json = load(file)
        CUSTOMER_ID = str(load_json['customer_id'])
        print(f"CUSTOMER ID: {CUSTOMER_ID}")
        CUSTOMER_NAME = str(load_json['customer_name'])
        print(f"CUSTOMER NAME: {CUSTOMER_NAME}")
        if isinstance(load_json, dict) and mode != 'BILLING':
            json_data["accounts"] = [load_json]
        else:
            json_data = load_json
    except Exception as e:
        print(f"Error loading data.json. ERROR: {e}")
    if mode == 'BILLING':
        uploads_dir = f'{main_dir}/uploads/{AUTOMATION}/{CUSTOMER_NAME}/{CUSTOMER_ID}'
    else:
        uploads_dir = f'{main_dir}/uploads/{AUTOMATION}/{CUSTOMER_NAME}/{CUSTOMER_ID}/{DATE_YEAR_MONTH}/{today_file}'
    logs_dir = f'{main_dir}/logs/{AUTOMATION}/{CUSTOMER_NAME}/{CUSTOMER_ID}/{DATE_YEAR_MONTH}/{today_file}'
    time_generated = getenv(
        'TIME_GENERATED_SCRIPT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    threads = int(getenv('THREADS_NUMBER', "6"))

    QUEUE_URI = getenv('QUEUE_URI', '')
    QUEUE_IDENTITY_ID = getenv('QUEUE_IDENTITY_ID', '')
    QUEUE_DESTINATION = getenv("QUEUE_DESTINATION", "posti")

    create_folder_if_not_exist([uploads_dir, logs_dir])
    log = cloudteam_logger.ct_logging(logs_dir, 'debug')
    try:
        queue_client = cloudteam_servicebus.ServiceBus(
            QUEUE_URI, QUEUE_IDENTITY_ID, log)
        log.info("Connected to queue service !")
    except Exception as e:
        log.error(f"Failed to connect to queue service: {e}")
        exit(1)

    if mode == 'INVENTORY':
        inventory_collector(uploads_directory=uploads_dir, logger=log,
                            accounts_json=json_data, time_generated=time_generated, threads=threads)
    elif mode == 'METRICS':
        metrics_file_path = '/app/files/metrics.json' if path.exists(
            '/app/files/metrics.json') else '/app/files/default_metrics.json'
        with open(metrics_file_path, encoding="UTF-8") as file:
            metrics_list = load(file)
        metrics_collector(uploads_directory=uploads_dir, logger=log,
                          accounts_json=json_data, time_generated=time_generated, metrics=metrics_list, threads=threads)
    elif mode == 'BILLING':
        billing_collector(uploads_directory=uploads_dir, logger=log,
                          account=json_data, time_generated=time_generated)
    elif mode == 'SIZING':
        sizing_collector(uploads_directory=uploads_dir, logger=log,
                         accounts_json=json_data, time_generated=time_generated, threads=threads)
    elif mode == 'ACCOUNTS':
        sizing_collector(uploads_directory=uploads_dir, logger=log,
                         accounts_json=json_data, time_generated=time_generated, threads=threads)
    queue_message = {
        'timegenerated': today,
        'automation': AUTOMATION,
        'customerid': CUSTOMER_ID,
        'uploads_dir': uploads_dir,
        'logs_dir': logs_dir,
    }
    queue_message_json = str(dumps(queue_message)).replace("'", '"')
    log.info(f"Sending message to queue. Payload:\n{queue_message_json}")
    queue_client.Send_Message(
        queueName=QUEUE_DESTINATION, message=queue_message_json)


if __name__ == '__main__':
    main()
