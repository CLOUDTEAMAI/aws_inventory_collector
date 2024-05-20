from datetime import datetime, timedelta
from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_xray(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('xray', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    while True:
        try:
            inventory = []
            response = client.get_trace_summaries(
                StartTime=start_time,
                EndTime=end_time,
                Sampling=False,  # Set to True to get a subset of traces if you're dealing with a high volume
                NextToken=next_token
            ) if next_token else client.get_trace_summaries(
                StartTime=start_time,
                EndTime=end_time,
                Sampling=False  # Set to True to get a subset of traces if you're dealing with a high volume
            )
            for resource in response.get('TraceSummaries', []):
                attributes = resource['Id']
                resource['StartTime'] = resource['StartTime'].isoformat()
                resource['MatchedEventTime'] = resource['MatchedEventTime'].isoformat()
                inventory_object = extract_common_info(
                    attributes, resource, region, account_id, time_generated, account_name)
                inventory.append(inventory_object)
            save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                str(stack()[0][3]), region, account_id, idx))
            next_token = response.get('NextToken', None)
            idx = idx + 1
            if not next_token:
                break
        except Exception as e:
            print(e)
            break
