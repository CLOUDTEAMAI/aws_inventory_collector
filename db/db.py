import pandas as pd
import psycopg2
import os




def create_table_in_psql(cur,table_name):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        arn TEXT UNIQUE,
        account_id TEXT,
        region TEXT,
        properties JSONB,
        timegenerated TIMESTAMP
    )
""")
os_env_dict = {
    'dbname'  : os.environ.get('POSTGRESSQL_DBNAME'),
    'host'    : os.environ.get('POSTGRESSQL_HOST'),
    'user'    : os.environ.get('POSTGRESSQL_USER'),
    'password': os.environ.get('POSTGRESSQL_PASSWORD'),
}

main_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(main_dir)
target_dir = f"{parent_dir}/uploads"
conn_params = f"dbname='{os_env_dict['dbname']}' user='{os_env_dict['user']}' host='{os_env_dict['host']}'" #password='your_pass'
conn = psycopg2.connect(conn_params)
cur = conn.cursor()
create_table_in_psql(cur,"cloudteamdb")


insert_query = """
    INSERT INTO cloudteamdb (arn, account_id, region, properties, timegenerated)
    VALUES (%s, %s, %s, %s, %s)
"""

insert_query_not_change_old_record = """
 INSERT INTO cloudteamdb (arn, account_id, region, properties, timegenerated)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (arn) DO NOTHING
"""
insert_query_update_old_record = """
    INSERT INTO cloudteamdb (arn, account_id, region, properties, timegenerated)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (arn) DO UPDATE SET
    account_id = EXCLUDED.account_id,
    region = EXCLUDED.region,
    properties = EXCLUDED.properties,
    timegenerated = EXCLUDED.timegenerated
"""


for filename in os.listdir(target_dir):
    if filename.endswith('.parquet'):
        filepath = os.path.join(target_dir, filename)
        df = pd.read_parquet(filepath)
        
        for index, row in df.iterrows():
            try:
                cur.execute(
                    insert_query_not_change_old_record, 
                    (row['arn'], row['account_id'], row['region'], row['properties'], row['timegenerated'])
                )
            except Exception as ex:
                print(ex)


conn.commit()
cur.close()
conn.close()

