import boto3
from utils.utils import *


def list_appstream(file_path, session, region):
    appstream = session.client("appstream", region_name=region)
    sts = session.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    inventory_instances = []
    inventory = appstream.describe_images()
    if len(inventory["Images"]) != 0:
        for i in inventory["Images"]:
            arn = i["Arn"]
            
            if "CreatedTime" in i:
                i["CreatedTime"] = i["CreatedTime"].isoformat()
            
            if "PublicBaseImageReleasedDate" in i:
                i["PublicBaseImageReleasedDate"] = i["PublicBaseImageReleasedDate"].isoformat()


            for cleaner in i.get("Applications",[]):
                if 'Metadata' in cleaner:
                    if cleaner['Metadata']['WORKING_DIRECTORY'] != None:
                        continue
                    else:
                        cleaner['Metadata']['WORKING_DIRECTORY'] = "null"

            inventory_object = extract_common_info(arn, i, region, account_id)
            inventory_instances.append(inventory_object)
        save_as_file_parquet(
            inventory_instances,
            file_path,
            generate_parquet_prefix(__file__, region, account_id),
        )
    return inventory_instances
