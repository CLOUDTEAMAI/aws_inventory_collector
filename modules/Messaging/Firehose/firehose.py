from inspect import stack
from utils.utils import extract_common_info, save_as_file_parquet, generate_parquet_prefix


def list_firehose(file_path, session, region, time_generated, account):
    next_token = None
    idx = 0
    client = session.client('firehose', region_name=region)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    streams = []
    while True:
        try:
            inventory = []
            response = client.list_delivery_streams(
                ExclusiveStartDeliveryStreamName=next_token) if next_token else client.list_delivery_streams()
            for stream in response.get('DeliveryStreamNames', []):
                streams.append(stream)
            next_token = response.get(
                'HasMoreDeliveryStreams', None)
            if not next_token:
                break
        except Exception as e:
            print(e)
            break

    for stream in streams:
        while True:
            try:
                HasMoreDestinations = None
                inventory = []
                response = client.describe_delivery_stream(
                    ExclusiveStartDestinationId=HasMoreDestinations) if HasMoreDestinations else client.describe_delivery_stream()
                for resource in response.get('DeliveryStreamDescription', []):
                    if 'CreateTimestamp' in resource:
                        resource['CreateTimestamp'] = resource['CreateTimestamp'].isoformat(
                        )
                    if 'LastUpdateTimestamp' in resource:
                        resource['LastUpdateTimestamp'] = resource['LastUpdateTimestamp'].isoformat(
                        )
                    if 'Source' in resource:
                        if 'DeliveryStartTimestamp' in resource['Source'].get('KinesisStreamSourceDescription', {}):
                            resource['Source']['KinesisStreamSourceDescription']['DeliveryStartTimestamp'] = resource[
                                'Source']['KinesisStreamSourceDescription']['DeliveryStartTimestamp'].isoformat()
                        if 'DeliveryStartTimestamp' in resource['Source'].get('KinesisStreamSourceDescription', {}):
                            resource['Source']['MSKSourceDescriptions']['DeliveryStartTimestamp'] = resource[
                                'Source']['MSKSourceDescriptions']['DeliveryStartTimestamp'].isoformat()

                    arn = resource['DeliveryStreamARN']
                    inventory_object = extract_common_info(
                        arn, resource, region, account_id, time_generated, account_name)
                    inventory.append(inventory_object)
                save_as_file_parquet(inventory, file_path, generate_parquet_prefix(
                    str(stack()[0][3]), region, account_id, idx))
                HasMoreDestinations = response.get(
                    'HasMoreDestinations', None)
                idx = idx + 1
                if not HasMoreDestinations:
                    break
            except Exception as e:
                print(e)
                break
