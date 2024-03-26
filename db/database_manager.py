import pandas as pd
import psycopg2
import os

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

    def create_table_collector(self, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            arn TEXT UNIQUE,
            account_id TEXT,
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
            label TEXT,
            properties JSONB,
            timegenerated TIMESTAMP
            
        )
        """
        self.cursor.execute(create_table_query)

    def insert_data_collector(self, table_name, data, update_on_conflict=False):
        if update_on_conflict:
            insert_query = f"""
            INSERT INTO {table_name} (arn, account_id, region, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (arn) DO UPDATE SET
            account_id = EXCLUDED.account_id,
            region = EXCLUDED.region,
            properties = EXCLUDED.properties,
            timegenerated = EXCLUDED.timegenerated
            """
        else:
            insert_query = f"""
            INSERT INTO {table_name} (arn, account_id, region, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (arn) DO NOTHING
            """

        for index, row in data.iterrows():
            try:
                self.cursor.execute(
                    insert_query,
                    (row['arn'], row['account_id'], row['region'], row['properties'], row['timegenerated'])
                )
            except Exception as ex:
                print(ex)
        self.connection.commit()
    def insert_data_metric(self, table_name, data, update_on_conflict=False):
        if update_on_conflict:
            insert_query = f"""
            INSERT INTO {table_name} (resource_id, account_id, label, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s)
            """
        else:
            insert_query = f"""
            INSERT INTO {table_name} (resource_id, account_id, label, properties, timegenerated)
            VALUES (%s, %s, %s, %s, %s)
            """

        for index, row in data.iterrows():
            try:
                self.cursor.execute(
                    insert_query,
                    (row['id'], row['account_id'], row['label'], row['properties'], row['timegenerated'])
                )
            except Exception as ex:
                print(ex)
        self.connection.commit()
    def load_data_from_parquet(self, target_dir):
        for filename in os.listdir(target_dir):
            if filename.endswith('.parquet'):
                filepath = os.path.join(target_dir, filename)
                df = pd.read_parquet(filepath)
                return df      
    def load_data_from_dir_parquet(selft,target_dir):
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



