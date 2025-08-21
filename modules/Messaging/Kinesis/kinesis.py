from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_kinesis(file_path, session, region, time_generated, account, boto_config):
    next_token = None
    idx = 0
    client = session.client('kinesis', region_name=region, config=boto_config)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    while True:
        try:
            inventory = []
            response = client.list_streams(
                NextToken=next_token) if next_token else client.list_streams()
            for resource in response.get('StreamNames', []):
                response_data = client.describe_stream(StreamName=resource)[
                    'StreamDescription']
                if 'StreamCreationTimestamp' in response_data:
                    response_data['StreamCreationTimestamp'] = response_data['StreamCreationTimestamp'].isoformat(
                    )
                arn = response_data['StreamARN']
                inventory_object = extract_common_info(
                    arn, response_data, region, account_id, time_generated, account_name)
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
