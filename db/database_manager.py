import pandas as pd
import psycopg2
import os
import abc



class DatabaseManager:
    def __init__(self):
        self.os_env_dict = {
            "dbname":  "eyaltest",#os.environ.get('POSTGRESSQL_DBNAME'),
            "host":    "localhost", #os.environ.get('POSTGRESSQL_HOST'),
            "user":     "eyalrainitz",#os.environ.get('POSTGRESSQL_USER'),
            "password": None #os.environ.get('POSTGRESSQL_PASSWORD')
        }
        self.conn_params = f"dbname='{self.os_env_dict['dbname']}' user='{self.os_env_dict['user']}' host='{self.os_env_dict['host']}' password='{self.os_env_dict['password']}'"
        self.connection = psycopg2.connect(self.conn_params)
        self.cursor = self.connection.cursor()
    
    def create_table_pricing(self, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            region TEXT,
            instance_type TEXT,
            family_instance TEXT,
            memory FLOAT,
            cpu FLOAT,
            cpu_architecture VARCHAR(50),
            price_per_hour FLOAT,
            price_per_day FLOAT,
            price_per_month FLOAT
        )
        """
        self.cursor.execute(create_table_query)

    def create_table_collector(self, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            arn TEXT UNIQUE,
            account_id TEXT,
            account_name TEXT,
            region TEXT,
            properties JSONB,
            timegenerated TIMESTAMP
        )
        """
        self.cursor.execute(create_table_query)
    
    def create_table_metric(self, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            resource_id TEXT,
            account_id TEXT,
            account_name TEXT,
            label TEXT,
            properties JSONB,
            timegenerated TIMESTAMP
            
        )
        """
        self.cursor.execute(create_table_query)
    
    def insert_data_collector(self, table_name, data, update_on_conflict=False):
        if update_on_conflict:
            insert_query = f"""
            INSERT INTO {table_name} (arn, account_id, account_name, region, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (arn) DO UPDATE SET
            account_id      = EXCLUDED.account_id,
            account_name    = EXCLUDED.account_name,
            region          = EXCLUDED.region,
            properties      = EXCLUDED.properties,
            timegenerated   = EXCLUDED.timegenerated
            """
        else:
            insert_query = f"""
            INSERT INTO {table_name} (arn, account_id,account_name, region, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (arn) DO NOTHING
            """

        for index, row in data.iterrows():
            try:
                self.cursor.execute(
                    insert_query,
                    (row['arn'], row['account_id'],row['account_name'], row['region'], row['properties'], row['timegenerated'])
                )
            except Exception as ex:
                print(ex)
        self.connection.commit()
    
    def insert_data_metric(self, table_name, data, update_on_conflict=False):
        if update_on_conflict:
            insert_query = f"""
            INSERT INTO {table_name} (resource_id, account_id,account_name, label, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
        else:
            insert_query = f"""
            INSERT INTO {table_name} (resource_id, account_id,account_name, label, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s, %s)
            """

        for index, row in data.iterrows():
            try:
                self.cursor.execute(
                    insert_query,
                    (row['id'], row['account_id'],row['account_name'], row['label'], row['properties'], row['timegenerated'])
                )
            except Exception as ex:
                print(ex)
        self.connection.commit()
    
    def insert_data_pricing(self, table_name, data, update_on_conflict=False):
        if update_on_conflict:
            insert_query = f"""
            INSERT INTO {table_name} (
            region,
            instance_type,
            family_instance,
            memory,
            cpu,
            cpu_architecture,
            price_per_hour,
            price_per_day,
            price_per_month)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            insert_query = f"""
            INSERT INTO {table_name} (
            region,
            instance_type,
            family_instance,
            memory,
            cpu,
            cpu_architecture,
            price_per_hour,
            price_per_day,
            price_per_month)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        for index, row in data.iterrows():
            try:
                self.cursor.execute(
                    insert_query,
                    (row['region'],
                      row['instance_type'], row['family_instance'],
                      row['memory'], row['cpu'],
                      row['cpu_architecture'], row['price_per_hour'],
                      row['price_per_day'], row['price_per_month']
                    )
                )
            except Exception as ex:
                print(ex)
        self.connection.commit()

    def get_stopped_ec2_ebs(self,table_name,target_dir,output_type="csv"):
        query_test  = f"""
SELECT 
    v.account_id,
    v.account_name,
    v.region,
    v.properties->>'VolumeId' AS volume_id,
	v.properties->>'VolumeType' AS volume_type,
	v.properties->>'Size' AS volume_size,
    i.arn AS instance_arn,
    instance_data-> 'State' ->> 'Name' AS instance_state,
    CASE 
        WHEN v.properties->>'VolumeType' = 'gp2' THEN (v.properties->>'Size')::numeric * 0.11
        WHEN v.properties->>'VolumeType' = 'gp3' THEN (v.properties->>'Size')::numeric * 0.08 -- Adjust price as necessary
        ELSE 0 -- Add other conditions for different volume types if necessary
    END AS volume_cost,
    (v.properties->>'Size')::numeric * 0.05 AS snapshot_cost -- Snapshot cost calculation
FROM 
    {table_name} v
CROSS JOIN LATERAL
    jsonb_array_elements(v.properties->'Attachments') AS attachment
JOIN LATERAL
    (SELECT
        arn,
        jsonb_array_elements(properties->'Instances') AS instance_data
     FROM 
        {table_name}
     WHERE arn LIKE '%instance%'
    ) AS i
ON attachment->>'InstanceId' = instance_data->>'InstanceId'
WHERE 
    instance_data-> 'State' ->> 'Name' != 'running'
ORDER BY volume_cost DESC
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_stopped_ec2_ebs.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_stopped_ec2_ebs.xlsx", index=False)
   
    def get_rds(self,table_name,target_dir,output_type="csv"):
        query_test  = f"""
SELECT 
account_id,
account_name,
region, 
arn, 
(properties->>'StorageType') as db_volume_type,
(properties->>'LicenseModel') as db_license_model,
(properties->>'DBInstanceClass') as db_instance_class,
(properties->>'Engine') as db_engine
FROM {table_name}
WHERE arn LIKE 'arn:aws:rds%'
order by account_id
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_rds.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_rds.xlsx", index=False)

    def get_autoscaling(self,table_name,target_dir,output_type="csv"):
        query_test  = f"""
SELECT 
    emea.account_id,
    emea.account_name,
    emea.region,
    split_part(emea.arn, '/', 2) AS resource_id,
    emea.properties->'DesiredCapacity' AS "DesiredCapacity",
    emea.properties->'MaxSize' AS "MaxSize",
    emea.properties->'MinSize' AS "MinSize",
    instance_type,
    instance_details.list_instances,
    emea.properties
FROM 
    {table_name} emea
CROSS JOIN LATERAL
    (SELECT jsonb_array_elements(emea.properties->'MixedInstancesPolicy'->'LaunchTemplate'->'Overrides')->>'InstanceType' AS instance_type) AS mit
LEFT JOIN LATERAL
    (SELECT array_agg(ARRAY[instance_data->>'InstanceId', instance_data->>'InstanceType']) AS list_instances
     FROM jsonb_array_elements(emea.properties->'Instances') AS instance_data) AS instance_details ON true
WHERE 
    emea.arn LIKE '%autoscaling%';
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_autoscaling.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_autoscaling.xlsx", index=False)
    
    def get_autoscaling_recommendation_with_metric(self,table_name,metric_table_name,target_dir,output_type="csv"):
        query_test  = f"""
  
  WITH Metrics AS (
    SELECT
        resource_id,
        account_id,
        account_name,
        label,
        ROUND(MAX(maximum) FILTER (WHERE days_ago <= 20), 2) AS max_20_days,
        ROUND(AVG(average) FILTER (WHERE days_ago <= 20), 2) AS avg_20_days,
        ROUND(MAX(maximum) FILTER (WHERE days_ago <= 40), 2) AS max_40_days,
        ROUND(AVG(average) FILTER (WHERE days_ago <= 40), 2) AS avg_40_days,
        ROUND(MAX(maximum) FILTER (WHERE days_ago <= 60), 2) AS max_60_days,
        ROUND(AVG(average) FILTER (WHERE days_ago <= 60), 2) AS avg_60_days,
        COUNT(*) FILTER (WHERE maximum BETWEEN 50 AND 60) AS count_50_60_max_cpu
    FROM (
        SELECT
            resource_id,
            account_id,
            label,
            (jsonb_array_elements(properties)->>'Timestamp')::timestamp WITH TIME ZONE AS metric_timestamp,
            ROUND((jsonb_array_elements(properties)->>'Average')::NUMERIC, 2) AS average,
            ROUND((jsonb_array_elements(properties)->>'Maximum')::NUMERIC, 2) AS maximum,
            EXTRACT(DAY FROM (CURRENT_TIMESTAMP - (jsonb_array_elements(properties)->>'Timestamp')::timestamp WITH TIME ZONE)) AS days_ago
        FROM 
            {metric_table_name}
        WHERE 
            properties::jsonb @> '[{{"Unit": "Percent"}}]' and label = 'CPUUtilization'
    ) AS Unnested
    GROUP BY 
        resource_id, account_id, label
),
AutoScalingInstances AS (
    SELECT 
        emea.account_id,
        emea.region,
        instance_data->>'InstanceId' AS instance_id,
        instance_data->>'InstanceType' AS instance_type,
        CASE 
            WHEN instance_data->>'InstanceType' SIMILAR TO 't[0-2]%' AND instance_data->>'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to t3 family'
            WHEN instance_data->>'InstanceType' SIMILAR TO 'c[0-4]%' AND instance_data->>'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to c6 family'
            WHEN instance_data->>'InstanceType' SIMILAR TO 'r[0-4]%' AND instance_data->>'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to r6 family'
            WHEN instance_data->>'InstanceType' SIMILAR TO 'm[0-4]%' AND instance_data->>'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to m6 family'
            WHEN instance_data->>'InstanceType' SIMILAR TO 't[3-4]%' OR instance_data->>'InstanceType' SIMILAR TO 'c[5-7]%' OR instance_data->>'InstanceType' SIMILAR TO 'r[5-7]%' OR instance_data->>'InstanceType' SIMILAR TO 'm[5-7]%' THEN 'Current generation'
            WHEN instance_data->>'InstanceType' SIMILAR TO 't[3-4]g%' OR instance_data->>'InstanceType' SIMILAR TO 'c[6-7]g%' OR instance_data->>'InstanceType' SIMILAR TO 'r[6-7]g%' OR instance_data->>'InstanceType' SIMILAR TO 'm[6-7]g%' THEN 'Not a candidate'
            WHEN instance_data->>'InstanceType' LIKE '%a' THEN 'Candidate for AMD'
            ELSE 'Not a candidate'
        END AS upgrade_option,
        CASE 
            WHEN instance_data->>'InstanceType' !~ '.*[ag]$' AND instance_data->>'InstanceType' !~ '.*[0-9]g.*' THEN true
            ELSE false
        END AS amd_candidate
    FROM 
        {table_name} emea,
        jsonb_array_elements(emea.properties->'Instances') AS instance_data
    WHERE 
        emea.arn LIKE '%autoscaling%'
)
SELECT 
    m.*,
    asi.region,
    asi.instance_id,
    asi.instance_type,
    asi.upgrade_option,
    asi.amd_candidate
FROM 
    Metrics m
JOIN 
    AutoScalingInstances asi ON m.resource_id = asi.instance_id
ORDER BY 
    m.account_id, asi.instance_type, asi.region, asi.upgrade_option;
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_autoscaling_recommendation_with_metric.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_autoscaling_recommendation_with_metric.xlsx", index=False)

    def get_gp2_to_gp3(self,table_name,target_dir,output_type="csv"):
        query_test  = f"""
SELECT
    account_id,
    account_name,
    region,
    arn,
    ebs_size,
    ebs_type,
    ebs_iops,
    ebs_throughput,
    gp2_price,
    gp3_price,
    (gp2_price - gp3_price) AS savings_if_moved_to_gp3
FROM (
    SELECT
        account_id,
        account_name,
        region,
        arn,
        properties->>'Size' AS ebs_size,
        properties->>'VolumeType' AS ebs_type,
        properties->>'Iops' AS ebs_iops,
        properties->>'Throughput' AS ebs_throughput,
        (properties->>'Size')::numeric * 0.11 AS gp2_price, -- Assuming $0.11/GB-month for gp2
        (properties->>'Size')::numeric * 0.088 + -- Base price for gp3
            CASE
                WHEN (properties->>'Iops')::numeric > 3000 THEN ((properties->>'Iops')::numeric - 3000) * 0.0055
                ELSE 0
            END +
            CASE
                WHEN (properties->>'Throughput')::numeric > 125 THEN ((properties->>'Throughput')::numeric - 125) * 0.044
                ELSE 0
            END AS gp3_price
    FROM
        {table_name}
    WHERE
        arn LIKE '%volume%'
) AS volume_data
WHERE
    ebs_type != 'gp3'
ORDER BY savings_if_moved_to_gp3 desc;
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_gp2_to_gp3.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_gp2_to_gp3.xlsx", index=False)

    def get_snapshots(self,table_name,target_dir,output_type="csv"):
        query_test  = f"""
SELECT 
    s.account_id,
    s.account_name,
    s.region,
    s.snapshot_id,
	s.create_time_snapshot,
    s.snapshot_name,
    s.volume_id,
    s.snapshot_size,
    v.vol_name,
-- 	v.size_gb,
	v.create_time_volume
FROM 
    (
        SELECT 
            account_id,
            account_name,
            region,
            split_part(arn, ':', 6) AS snapshot_id,
            tag->>'Value' AS snapshot_name,
            properties->>'VolumeId' as volume_id,
            properties->>'VolumeSize' as snapshot_size,
			properties->>'StartTime' as create_time_snapshot
        FROM 
            {table_name},
            jsonb_array_elements(properties->'Tags') as tag
        WHERE arn LIKE '%snap%' and tag->>'Key' = 'Name'
    ) AS s
JOIN 
    (
        SELECT 
            account_id,
            account_name,
            region,
            properties->>'VolumeId' as vol_id,
			properties->>'CreateTime' as create_time_volume,
			properties->>'Size' as size_gb,
            tag->>'Value' AS vol_name
        FROM
            {table_name},
            jsonb_array_elements(properties->'Tags') as tag
        WHERE arn like '%volume%' and tag->>'Key' = 'Name'
    ) AS v
ON s.volume_id = v.vol_id AND s.account_id = v.account_id AND s.region = v.region
order by vol_name
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_snapshots.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_snapshots.xlsx", index=False)

    def get_snapshots_price(self,table_name,target_dir,output_type="csv"):
        query_test  = f"""
select account_id,
account_name,
region,
-- arn,
split_part(arn, ':', 6) AS snapshot_id,
(properties->>'VolumeId') as volume_id,
(properties->>'VolumeSize')::int as volume_size_gb,
(properties->>'StartTime')::date as snapshot_time,
(properties->>'VolumeSize')::int * 0.05 AS snapshot_price
from {table_name}
where arn like '%snap-%'
order by snapshot_time ASC;
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_snapshots_price.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_snapshots_price.xlsx", index=False)

    def get_ec2_recommendations_with_metric(self,table_name,metric_table_name,target_dir,output_type="csv"):
        query_test  = f"""
WITH Unnested AS (
    SELECT
        resource_id,
        label,
        account_id,
        account_name,
        (jsonb_array_elements(properties)->>'Timestamp')::timestamp WITH TIME ZONE AS metric_timestamp,
        ROUND((jsonb_array_elements(properties)->>'Average')::NUMERIC, 2) AS average,
        ROUND((jsonb_array_elements(properties)->>'Maximum')::NUMERIC, 2) AS maximum
    FROM 
        {metric_table_name}
    WHERE 
        properties::jsonb @> '[{{"Unit": "Percent"}}]' and label = 'CPUUtilization'
),
Filtered AS (
    SELECT
        resource_id,
        account_id,
        account_name,
        label,
        average,
        maximum,
        EXTRACT(DAY FROM (CURRENT_TIMESTAMP - metric_timestamp)) AS days_ago
    FROM Unnested
),
Metrics AS (
    SELECT
        resource_id,
        account_id,
        account_name,
        label,
        ROUND(MAX(maximum) FILTER (WHERE days_ago <= 20), 2) AS max_20_days,
        ROUND(AVG(average) FILTER (WHERE days_ago <= 20), 2) AS avg_20_days,
        ROUND(MAX(maximum) FILTER (WHERE days_ago <= 40), 2) AS max_40_days,
        ROUND(AVG(average) FILTER (WHERE days_ago <= 40), 2) AS avg_40_days,
        ROUND(MAX(maximum) FILTER (WHERE days_ago <= 60), 2) AS max_60_days,
        ROUND(AVG(average) FILTER (WHERE days_ago <= 60), 2) AS avg_60_days,
        COUNT(*) FILTER (WHERE maximum BETWEEN 50 AND 60) AS count_50_60_max_cpu
    FROM 
        Filtered
    GROUP BY 
        resource_id, account_id, label
),
Instances AS (
    SELECT 
        account_id,
        account_name,
        region,
        split_part(arn, '/', 2) AS instance_id,
        properties_data ->> 'InstanceType' AS instance_type,
        properties_data -> 'State' ->> 'Name' AS state,
        properties_data ->> 'PlatformDetails' AS os_type,
        tag->>'Value' AS instance_name,
        CASE 
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 't[0-2]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to t3 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 'c[0-4]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to c6 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 'r[0-4]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to r6 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 'm[0-4]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to m6 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 't[3-4]%' OR properties_data ->> 'InstanceType' SIMILAR TO 'c[5-7]%' OR properties_data ->> 'InstanceType' SIMILAR TO 'r[5-7]%' OR properties_data ->> 'InstanceType' SIMILAR TO 'm[5-7]%' THEN 'Current generation'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 't[3-4]g%' OR properties_data ->> 'InstanceType' SIMILAR TO 'c[6-7]g%' OR properties_data ->> 'InstanceType' SIMILAR TO 'r[6-7]g%' OR properties_data ->> 'InstanceType' SIMILAR TO 'm[6-7]g%' THEN 'Not a candidate'
            WHEN properties_data ->> 'InstanceType' LIKE '%a' THEN 'Candidate for AMD'
            ELSE 'Not a candidate'
        END AS upgrade_option,
        CASE 
            WHEN properties_data ->> 'InstanceType' !~ '.*[ag]$' AND properties_data ->> 'InstanceType' !~ '.*[0-9]g.*' THEN true
            ELSE false
        END AS amd_candidate
    FROM 
        {table_name},
        jsonb_array_elements(properties->'Instances') AS properties_data,
        jsonb_array_elements(properties_data->'Tags') AS tag
    WHERE tag->>'Key' = 'Name'
)
SELECT 
    m.*,
    i.region,
    i.instance_id,
    i.instance_name,
    i.instance_type,
    i.state,
    i.os_type,
    i.upgrade_option,
    i.amd_candidate,
--     p.price_per_hour,
    ROUND((p.price_per_hour * 24)::NUMERIC, 2) AS price_per_day,
    ROUND((p.price_per_hour * 24 * 30.4)::NUMERIC, 2) AS price_per_month,
--     amd_p.price_per_hour AS amd_price_per_hour,
    ROUND((amd_p.price_per_hour * 24)::NUMERIC, 2) AS amd_price_per_day,
    ROUND((amd_p.price_per_hour * 24 * 30.4)::NUMERIC, 2) AS amd_price_per_month,
	ROUND((amd_p.price_per_hour * 24 * 30.4/2)::NUMERIC, 2) AS amd_changed_price_per_month,
	ROUND(((p.price_per_hour * 24 * 30.4)-(amd_p.price_per_hour * 24 * 30.4/2))::NUMERIC, 2) AS in_change_size_saving,
	ROUND(((p.price_per_hour * 24 * 30.4)-(amd_p.price_per_hour * 24 * 30.4))::NUMERIC, 2) AS in_change_to_amd_saving
FROM 
    Metrics m
JOIN 
    Instances i ON m.resource_id = i.instance_id AND m.account_id = i.account_id
JOIN
    ec2_pricing_table p ON i.instance_type = p.instance_type AND i.region = p.region
LEFT JOIN
    ec2_pricing_table amd_p ON CONCAT(SPLIT_PART(i.instance_type, '.', 1), 'a.', SPLIT_PART(i.instance_type, '.', 2)) = amd_p.instance_type AND i.region = amd_p.region
ORDER BY 
    m.account_id, i.upgrade_option,i.instance_type,i.region, i.amd_candidate DESC, m.max_20_days, m.label;
"""     
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_ec2_recommendations_with_metric.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_ec2_recommendations_with_metric.xlsx", index=False)

    def get_ec2_recoomendations_by_generation(self,table_name,target_dir,output_type="csv"):
        query_test = f"""
SELECT 
        account_id,
        account_name,
        region,
        split_part(arn, '/', 2) AS instance_id,
        properties_data ->> 'InstanceType' AS instance_type,
        properties_data -> 'State' ->> 'Name' AS state,
        properties_data ->> 'PlatformDetails' AS os_type,
        tag->>'Value' AS instance_name,
        CASE 
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 't[0-2]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to t3 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 'c[0-4]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to c6 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 'r[0-4]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to r6 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 'm[0-4]%' AND properties_data ->> 'InstanceType' NOT LIKE '%g' THEN 'Upgrade recommended to m6 family'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 't[3-4]%' OR properties_data ->> 'InstanceType' SIMILAR TO 'c[5-7]%' OR properties_data ->> 'InstanceType' SIMILAR TO 'r[5-7]%' OR properties_data ->> 'InstanceType' SIMILAR TO 'm[5-7]%' THEN 'Current generation'
            WHEN properties_data ->> 'InstanceType' SIMILAR TO 't[3-4]g%' OR properties_data ->> 'InstanceType' SIMILAR TO 'c[6-7]g%' OR properties_data ->> 'InstanceType' SIMILAR TO 'r[6-7]g%' OR properties_data ->> 'InstanceType' SIMILAR TO 'm[6-7]g%' THEN 'Not a candidate'
            WHEN properties_data ->> 'InstanceType' LIKE '%a' THEN 'Candidate for AMD'
            ELSE 'Not a candidate'
        END AS upgrade_option,
        CASE 
            WHEN properties_data ->> 'InstanceType' !~ '.*[ag]$' AND properties_data ->> 'InstanceType' !~ '.*[0-9]g.*' THEN true
            ELSE false
        END AS amd_candidate
    FROM 
        {table_name},
        jsonb_array_elements(properties->'Instances') AS properties_data,
        jsonb_array_elements(properties_data->'Tags') AS tag
    WHERE tag->>'Key' = 'Name'
"""
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/get_ec2_recoomendations_by_generation.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/get_ec2_recoomendations_by_generation.xlsx", index=False)

    def load_data_from_dir_parquet(self,target_dir):
        dataframes = []
        for filename in os.listdir(target_dir):
            if filename.endswith('.parquet'):
                filepath = os.path.join(target_dir, filename)
                df = pd.read_parquet(filepath)
                dataframes.append(df)
        if dataframes:
            combined_df = pd.concat(dataframes,ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()
        
    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def generate_data_from_all_queries(self, table_name, target_dir, metric_table_name, output_type="csv"):
        function_to_exe = [
            (self.get_stopped_ec2_ebs,                        [table_name, target_dir, output_type]),
            (self.unattached_eip,                             [table_name, target_dir, output_type]),
            (self.get_rds,                                    [table_name, target_dir, output_type]),
            (self.get_autoscaling,                            [table_name, target_dir, output_type]),
            # (self.get_autoscaling_recommendation_with_metric, [table_name, metric_table_name, target_dir, output_type]),
            (self.get_gp2_to_gp3,                             [table_name, target_dir, output_type]),
            (self.get_snapshots,                              [table_name, target_dir, output_type]),
            (self.get_snapshots_price,                        [table_name, target_dir, output_type]),
            # (self.get_ec2_recommendations_with_metric,        [table_name, metric_table_name, target_dir, output_type]),
            (self.get_ec2_recoomendations_by_generation,      [table_name, target_dir, output_type])
        ]

        for func, args in function_to_exe:
            func(*args)

    def unattached_eip(self,table_name,target_dir,output_type="csv"):
        query_test = f"""
SELECT * FROM {table_name}
WHERE arn LIKE '%eip%' AND (properties->> 'NetworkInterfaceId' IS NUll OR properties->> 'AssociationId' IS NULL)
"""
        df = pd.read_sql_query(query_test,self.connection)
        if output_type == "csv":
            df.to_csv(f"{target_dir}/unattached_eip.csv",index=False)
        elif output_type == "xlsx":
            df.to_excel(f"{target_dir}/unattached_eip.xlsx", index=False)
# def load_data_from_parquet(self, target_dir):
    #     for filename in os.listdir(target_dir):
    #         if filename.endswith('.parquet'):
    #             filepath = os.path.join(target_dir, filename)
    #             df = pd.read_parquet(filepath)
    #             return df    
    