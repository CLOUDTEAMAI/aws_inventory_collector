from datetime import datetime
from os import getenv
from re import search
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from .billing import *
from utils.utils import create_folder_if_not_exist


def billing_collector(uploads_directory, logger, account, time_generated):
    """
    The `billing_collector` function downloads specific files from an AWS S3 bucket based on certain
    criteria and logs any errors encountered during the process.

    :param uploads_directory: The `uploads_directory` parameter in the `billing_collector` function is a
    directory path where the downloaded billing files will be stored. It is the location on the local
    system where the billing files from the AWS S3 bucket will be saved after downloading
    :param logger: The `logger` parameter in the `billing_collector` function is typically an object
    that is used for logging messages, errors, and other information during the execution of the
    function. It allows you to track the progress of the function, handle exceptions, and provide
    insights into what is happening within the function
    :param account: The `account` parameter in the `billing_collector` function seems to be a dictionary
    containing information related to an AWS account. It likely includes details such as the account ID,
    region, role name, bucket name, start date, and a flag indicating whether to collect all files. This
    information is
    :param time_generated: The `time_generated` parameter in the `billing_collector` function is used to
    specify the time at which the billing collection process is being initiated. It is expected to be a
    timestamp or datetime object representing the time when the function is called. This parameter is
    used in the function to determine the start
    """
    lock = Lock()
    files = []
    print(f"Account: {account}")
    session = get_aws_session(
        account_id=account['account_id'], region=account.get('region', 'us-east-1'), role_name=account.get('account_role', 'Cloudteam-FinOps'))

    s3_manager = S3Manager(session=session, bucket_name=account['bucket_name'])
    files_mapping = []
    # List files
    files = s3_manager.list_files()
    if files:
        if account.get('collect_all', 'false') == 'true' or getenv('COLLECT_ALL', 'false') == 'true':
            for file in files:
                if (file.endswith('.parquet') or file.endswith('.csv') or file.endswith('.csv.gz')) and ("cost_and_usage_data_status" not in file):
                    file_replaced = file.replace('/', '-')
                    new_file = file_replaced
                    matching = search(r'year=(\d+)-month=(\d+)', file_replaced)
                    if matching:
                        year, month = matching.groups()
                        month = month.zfill(2)  # Ensure two-digit month format
                        new_file = f"{year}-{month}/{file_replaced}"

                    files_mapping.append({
                        "source": file,
                        "target": f"{uploads_directory}/{new_file}"
                    })
        else:
            scan_date = datetime.strptime(account.get(
                'start_date', time_generated), "%Y-%m-%d")
            date_info = [f'year={scan_date.year}',
                         f'month={scan_date.month}']
            date_info2 = f"{scan_date.year}{month_formatter(scan_date.month)}01-"
            if scan_date.month == 12:
                date_info2 = date_info2 + f"{scan_date.year + 1}0101"
            else:
                date_info2 = date_info2 + \
                    f"{scan_date.year}{month_formatter(scan_date.month + 1)}01"
            for file in files:
                file_splitted = file.lower().split('/')
                if ((date_info2 in file_splitted) or (all(item in file_splitted for item in date_info))) and (file.endswith('.parquet') or file.endswith('.csv') or file.endswith('.csv.gz')):
                    files_mapping.append({
                        "source": file,
                        "target": f"{uploads_directory}/{file.replace('/', '-')}"
                    })
        sub_folders = []
        for folder in files_mapping:
            sub_folder_name = "/".join(folder['target'].split(
                f"{uploads_directory}/")[-1].split("/")[:-1])
            sub_folders.append(f"{uploads_directory}/{sub_folder_name}")
        print(f"Subfolders to be created: \n{sub_folders}")
        for folder in list(set(sub_folders)):
            print(f"Creating {folder} subfolder...")
            create_folder_if_not_exist([folder])
            print(f"{folder} subfolder created.")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(
                s3_manager.download_file, file['source'], file['target']): file for file in files_mapping}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as e:
                    with lock:
                        logger.error(f"(URL:{url}): {str(e)}")
    else:
        logger.error(
            f"Unable to list files in {account['bucket_name']} bucket")
