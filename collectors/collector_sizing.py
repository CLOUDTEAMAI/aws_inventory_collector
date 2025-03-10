from concurrent.futures import as_completed, ThreadPoolExecutor
from threading import Lock
from .sizing import *

lock = Lock()


def sizing_collector(uploads_directory, logger, accounts_json, time_generated, threads=4):
    # regional scraping per account
    try:
        get_all_accounts_regional_sizing(main_dir=uploads_directory, logger_obj=logger,
                                         account_json=accounts_json, time_generated=time_generated, threads=threads)
        print("Finished Collecting regional sizing")
    except Exception as ex:
        print(f"Failed to execute get_all_accounts_regional_sizing \n{ex}")


def get_all_accounts_regional_sizing(logger_obj, main_dir: str, account_json: list, time_generated, threads=4):
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures_services = {
                executor.submit(
                    lambda acc=account, reg=region: parallel_executor_regional_sizing(
                        logger_obj, main_dir,
                        get_aws_session(acc['account_id'],
                                        reg, role_name=acc['account_role']),
                        reg,
                        time_generated,
                        complete_aws_account(acc),
                        threads
                    ), account, region  # Defaulting acc and reg inside lambda
                ): account for account in account_json['accounts'] for region in ['us-east-1'] if get_aws_session(account['account_id'], role_name=account.get('account_role', 'Cloudteam-FinOps')) is not None
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


def parallel_executor_regional_sizing(logger_obj, main_dir: str, session, region: str, time_generated, account, threads=4):
    tasks = {
        'rds_sizing': list_rds_sizing,
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
