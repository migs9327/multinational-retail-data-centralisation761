#%%
import pandas as pd
import tabula
import requests
import concurrent.futures
import boto3
#%%
class DataExtractor:
    """
    A class for extracting data from various sources like databases, PDFs, APIs, and S3 buckets.
    """

    def __init__(self, database_connector = None, num_stores_endpoint=None, store_endpoint=None, header_dict=None, datetime_endpoint=None):
        """
        Initializes the DataExtractor with database connector and API details.

        Args:
            database_connector (DatabaseConnector): Connector for database operations.
            num_stores_endpoint (str): API endpoint to get the number of stores.
            store_endpoint (str): API endpoint to get store details.
            header_dict (dict): Headers for API requests.
            datetime_endpoint (str): API endpoint for datetime data.
        """
        self.database_connector = database_connector
        self.header_dict = header_dict
        self.num_stores_endpoint = num_stores_endpoint
        self.store_endpoint = store_endpoint
        self.datetime_endpoint = datetime_endpoint

    def read_rds_table(self, table_name):
        """
        Extracts a database table into a DataFrame.

        Args:
            table_name (str): Name of the table to be extracted.

        Returns:
            DataFrame: The extracted table as a DataFrame.
        """
        table_df = pd.read_sql_table(table_name=table_name, con=self.database_connector.init_db_engine())
        return table_df
    
    def retrieve_pdf_data(self, url):
        """
        Extracts data from a PDF file into a DataFrame.

        Args:
            url (str): URL of the PDF file.

        Returns:
            DataFrame: The extracted PDF data as a DataFrame.
        """
        df = tabula.read_pdf(url, pages='all')
        merged_df = pd.concat(df)
        merged_df.reset_index(drop=True, inplace=True)
        return merged_df
    
    def list_number_of_stores(self):
        """
        Retrieves the number of stores from the API endpoint.

        Returns:
            int: The number of stores, or 0 if the API request fails.
        """
        response = requests.get(self.num_stores_endpoint, headers=self.header_dict)
        if response.status_code == 200:
            return response.json().get('number_stores', 0)
        else:
            print("Failed to retrieve number of stores")
            return 0

    def fetch_url(self, url_and_header_dict):
        """
        Fetches data from a given URL with specified headers.

        Args:
            url_and_header_dict (tuple): Tuple containing the URL and header dictionary.

        Returns:
            dict or None: The JSON response as a dictionary, or None if the request fails.
        """
        url, header_dict = url_and_header_dict
        try:
            response = requests.get(url, headers=header_dict, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def retrieve_stores_data(self):
        """
        Retrieves data for all stores by making concurrent API calls.

        Returns:
            DataFrame: DataFrame containing information for all stores.
        """
        urls = [f"{self.store_endpoint}{store_number}" for store_number in range(self.list_number_of_stores())]
        args = [(url, self.header_dict) for url in urls]
        store_info_dict = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            results = executor.map(self.fetch_url, args)
            for store_number, store_info in zip(range(self.list_number_of_stores()), results):
                if store_info is not None:
                    store_info_dict[store_number] = store_info

        store_info_df = pd.DataFrame.from_dict(store_info_dict, orient='index')
        
        return store_info_df
    
    def retrieve_datetime_data(self):
        """
        Retrieves datetime data from the specified API endpoint.

        Returns:
            DataFrame: DataFrame containing the datetime data.
        """
        datetime_response = requests.get(self.datetime_endpoint, json=True)
        datetime_dict = datetime_response.json()
        datetime_df = pd.DataFrame(datetime_dict)
        datetime_df.index = datetime_df.index.astype(int)
        return datetime_df


    @staticmethod
    def extract_from_s3(s3_address):
        """
        Extracts data from a given S3 bucket address.

        Args:
            s3_address (str): The S3 bucket address.

        Returns:
            DataFrame: DataFrame containing the data extracted from the S3 bucket.
        """
        s3 = boto3.client('s3')
        s3_address_split = s3_address.split('/')
        product_data_obj_key = s3_address_split[-1] # object key references what's inside the S3
        bucket_name = s3_address_split[-2]
        s3.download_file(bucket_name, product_data_obj_key, 'products.csv')
        products_df = pd.read_csv('products.csv', index_col='Unnamed: 0')
        return products_df
    
