from json import load
from os import path, environ
from datetime import datetime
from cloudteam_logger import cloudteam_logger
from collectors.collector_inventory import inventory_collector
from collectors.collector_metrics import metrics_collector
from utils.utils import create_folder_if_not_exist


def main():
    """
    The main function reads account information from a JSON file, creates AWS sessions for each account,
    and then runs parallel tasks to gather inventory and list S3 buckets for each account.
    """
    # arranging all os configs such as path of file runing or create folders if not exist
    mode = environ.get('MODE', 'inventory')
    main_dir = path.dirname(path.abspath(__file__))
    uploads = f'{main_dir}/uploads'
    create_folder_if_not_exist([f'{main_dir}/uploads', f'{main_dir}/files',
                               f'{main_dir}/logs'])
    time_generated = environ.get(
        'TIME_GENERATED_SCRIPT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    threads = int(environ.get('THREADS_NUMBER', 4))
    logger_obj = cloudteam_logger.ct_logging(f'{main_dir}/logs', 'debug')

    with open(f'{main_dir}/files/accounts.json', encoding="UTF-8") as file:
        load_json = load(file)

    if mode.lower() == 'inventory':
        inventory_collector(uploads_directory=uploads, logger=logger_obj,
                            accounts_json=load_json, time_generated=time_generated, threads=threads)
    elif mode.lower() in ('metric', 'metrics'):
        with open(f'{main_dir}/files/metrics.json', encoding="UTF-8") as file:
            metrics_list = load(file)
        metrics_collector(uploads_directory=uploads, logger=logger_obj,
                          accounts_json=load_json, time_generated=time_generated, metrics=metrics_list, threads=threads)


if __name__ == '__main__':
    main()
