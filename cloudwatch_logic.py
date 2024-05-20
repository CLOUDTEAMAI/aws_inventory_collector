import json
import boto3
import concurrent.futures
from datetime import datetime, timedelta
from utils.utils import extract_common_info_metrics


def get_resource_utilization_metric(session, ids: str, metricname: str, statistics: list, unit: str, name_dimensions: str, serviceType: str, account, days=30):
    client = session.client('cloudwatch', region_name=region_name)
    account_id = account['account_id']
    account_name = str(account['account_name']).replace(" ", "_")
    start_timer = datetime.now()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days)

    response = client.get_metric_data(
        MetricDataQueries=query,
        StartTime=start_time,
        EndTime=end_time
    )
    for i in response['Datapoints']:
        i['Timestamp'] = i['Timestamp'].isoformat()

    item = extract_common_info_metrics(
        account_id, ids, response['Datapoints'], response['Label'], timegenerated=None, account_name=account_name)
    stop_timer = datetime.now()
    runtime = (stop_timer - start_timer).total_seconds()
    print(f"{runtime % 60 :.3f}")
    return item
