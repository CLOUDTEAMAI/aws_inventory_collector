from datetime import datetime
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from .billing import *


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
    scan_date = datetime.strptime(account.get(
        'start_date', time_generated), "%Y-%m-%d")
    session = get_aws_session(
        account_id=account['account_id'], region=account.get('region', 'us-east-1'), role_name=account['account_role'])

    s3_manager = S3Manager(session, account['bucket_name'])
    files_mapping = []
    # List files
    files = s3_manager.list_files()
    if files:
        if account.get('collect_all', 'false') == 'true':
            for file in files:
                if (file.endswith('.parquet') or file.endswith('.csv') or file.endswith('.csv.gz')):
                    files_mapping.append({
                        "source": file,
                        "target": f"{uploads_directory}/{file.replace('/', '-')}"
                    })
        else:
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
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(
                s3_manager.download_file, file['source'], file['target']): file for file in files_mapping}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as e:
                    with lock:
                        logger.error(f"ERROR (URL:{url}): {str(e)}")
    else:
        logger.error(
            f"Unable to list files in {account['bucket_name']} bucket")
