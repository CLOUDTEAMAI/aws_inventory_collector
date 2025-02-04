from concurrent.futures import as_completed, ThreadPoolExecutor
from threading import Lock
from .inventory import *

lock = Lock()


def inventory_collector(uploads_directory, logger, accounts_json, time_generated, threads=4):
    # regional scraping per account
    try:
        get_all_accounts_regional_inventory(main_dir=uploads_directory, logger_obj=logger,
                                            account_json=accounts_json, time_generated=time_generated, threads=threads)
        print("Finished Collecting regional inventory")
    except Exception as ex:
        print(f"Failed to execute get_all_accounts_regional_inventory \n{ex}")


def get_all_accounts_regional_inventory(logger_obj, main_dir: str, account_json: list, time_generated, threads=4):
    """
    The function `get_all_accounts_regional_inventory` uses ThreadPoolExecutor to concurrently retrieve
    regional inventory data for multiple AWS accounts and regions.

    :param logger_obj: The `logger_obj` parameter is an object that is used for logging messages and
    errors in the code. It is typically an instance of a logging class that allows you to record events
    that occur during the program's execution. This object helps in tracking and debugging the code by
    providing a way to store and
    :param main_dir: The `main_dir` parameter in the `get_all_accounts_regional_inventory` function is a
    string that represents the main directory where the inventory data will be stored or retrieved from.
    It is used as a path or directory location within the file system where the function will read or
    write data related to the
    :type main_dir: str
    :param account_json: The `account_json` parameter in the `get_all_accounts_regional_inventory`
    function seems to be a list of dictionaries. Each dictionary in the list likely represents an
    account with keys like 'account_id' and 'account_role'. This list is used to iterate over accounts
    and their regions to perform some
    :type account_json: list
    :param time_generated: The `time_generated` parameter in the `get_all_accounts_regional_inventory`
    function seems to be used to specify a timestamp or time-related information. This parameter likely
    indicates the time at which the inventory data is being generated or collected for the accounts and
    regions
    :param threads: The `threads` parameter in the `get_all_accounts_regional_inventory` function
    specifies the maximum number of threads that can be used by the ThreadPoolExecutor for executing the
    tasks concurrently. In this case, the default value for `threads` is set to 4, meaning that at most
    4 tasks, defaults to 4 (optional)
    """
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures_services = {
                executor.submit(
                    lambda acc=account, reg=region: parallel_executor_regional_inventory(
                        logger_obj, main_dir,
                        get_aws_session(acc['account_id'],
                                        reg, role_name=acc['account_role']),
                        reg,
                        time_generated,
                        complete_aws_account(acc),
                        threads
                    ), account, region  # Defaulting acc and reg inside lambda
                ): account for account in account_json for region in regions_enabled(get_aws_session(account['account_id'], role_name=account['account_role'])) if get_aws_session(account['account_id'], role_name=account['account_role']) is not None
            }
            for future in as_completed(futures_services):
                try:
                    result = future.result()  # Corrected to call .result() on future
                    # Process result or print
                    del result
                except Exception as ex:
                    print(f'Error raised in \n {ex}')
                    with lock:
                        logger_obj.error(str(ex))
    except Exception as ex:
        print(f'Error in end {ex}')


def parallel_executor_regional_inventory(logger_obj, main_dir: str, session, region: str, time_generated: datetime, account, threads=4):
    """
    The function `parallel_executor_regional_inventory` executes multiple resource listing functions in
    parallel for a specified region and account.

    :param logger_obj: The `logger_obj` parameter in the `parallel_executor_regional_inventory` function
    is an object that handles logging messages and errors during the execution of the function. It is
    used to log information about the tasks being executed, any exceptions that occur, and other
    relevant details for monitoring and debugging purposes
    :param main_dir: The `main_dir` parameter in the `parallel_executor_regional_inventory` function is
    a string that represents the main directory path where the function will list resources from various
    services in parallel. This directory path is used as a reference point for accessing and organizing
    the resources being listed by the function
    :type main_dir: str
    :param session: The `session` parameter in the `parallel_executor_regional_inventory` function is
    typically an object representing a session with the AWS services. It is commonly used to make API
    calls to AWS services within the specified region. This session object usually contains the
    necessary credentials and configuration settings required to authenticate and interact with
    :param region: The `region` parameter in the `parallel_executor_regional_inventory` function is used
    to specify the AWS region for which you want to list resources. This function is designed to list
    resources from various AWS services in parallel for the specified region. The function contains a
    mapping of service names to corresponding list functions
    :type region: str
    :param time_generated: The `time_generated` parameter in the `parallel_executor_regional_inventory`
    function is expected to be of type `datetime`. This parameter represents the timestamp or datetime
    at which the inventory is being generated or processed. It is used within the function to provide
    context or time reference for the inventory operations being performed
    :type time_generated: datetime
    :param account: The `account` parameter in the `parallel_executor_regional_inventory` function seems
    to represent an account object or dictionary containing information about an AWS account. It likely
    includes details such as the account ID, account name, and possibly other account-specific
    information needed for listing AWS resources in parallel for that account within
    :param threads: The `threads` parameter in the `parallel_executor_regional_inventory` function
    specifies the number of threads to use for parallel execution of tasks. Increasing the number of
    threads can potentially improve performance by allowing multiple tasks to run concurrently. However,
    it's important to consider the resources available and the nature of the, defaults to 4 (optional)
    """
    # Initialize functions clients for services you want to list resources from in parallel
    tasks = {
        'account_name': list_account_name,
        'accounts': list_accounts,
        'organization': list_org,
    }
    with ThreadPoolExecutor(threads) as executor:
        future_to_task = {
            executor.submit(task, main_dir, session, region, time_generated, account): name for name, task in tasks.items()
        }
        for future in as_completed(future_to_task):
            task_name = future_to_task[future]
            try:
                data = future.result()
                print(f"{task_name} completed {region}, {data}")
                del data
            except Exception as exc:
                print(f"{task_name} generated an exception: {exc}")
                account_id = account['account_id']
                with lock:
                    logger_obj.error(
                        f'{account_id} {region} {task_name} {str(exc)}')
                del exc
        del future_to_task
