#%%
import yaml
from sqlalchemy import create_engine, inspect
#%%
class DatabaseConnector:
    """
    A class for handling database connections and operations.

    Attributes:
        db_creds_path (str): Path to the database credentials YAML file.
    """

    def __init__(self, db_creds_path=None, ):
        """
        Initializes the DatabaseConnector with the path to the database credentials.

        Args:
            db_creds_path (str): Path to the database credentials file.
        """
        self.db_creds_path = db_creds_path

    def read_db_creds(self):
        """
        Reads the database credentials from a YAML file.

        Returns:
            dict: Database credentials.
        """
        if self.db_creds_path:
            with open(self.db_creds_path, 'r') as stream:
                creds_dict = yaml.load(stream, Loader=yaml.FullLoader)
            return creds_dict

    def init_db_engine(self):
        """
        Initializes and returns a SQLAlchemy database engine using credentials.

        Returns:
            Engine: SQLAlchemy database engine.
        """
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
        """
        Lists all tables in the database.

        Returns:
            list: List of table names in the database.
        """
        engine = self.init_db_engine()
        with engine.execution_options().connect() as conn:
            inspector = inspect(conn)
            table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df_to_upload, upload_table_name):
        """
        Uploads a DataFrame to a specific table in the database.

        Args:
            df_to_upload (DataFrame): The DataFrame to upload.
            upload_table_name (str): The name of the target table in the database.
        """
        engine = self.init_db_engine()
        df_to_upload.to_sql(upload_table_name, engine, if_exists='replace')