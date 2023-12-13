#%%
import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
#%%
class DatabaseConnector:

    def __init__(self, db_creds_path=None, ):
        self.db_creds_path = db_creds_path

    def read_db_creds(self):
        if self.db_creds_path:
            with open(self.db_creds_path, 'r') as stream:
                creds_dict = yaml.load(stream, Loader=yaml.FullLoader)
            return creds_dict

    def init_db_engine(self):
        creds_dict = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds_dict['HOST']
        USER = creds_dict['USER']
        PASSWORD = creds_dict['PASSWORD']
        DATABASE = creds_dict['DATABASE']
        PORT = creds_dict['PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            #TODO conn isn't used - fix
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df_to_upload, upload_table_name):
        engine = self.init_db_engine()
        df_to_upload.to_sql(upload_table_name, engine, if_exists='replace')