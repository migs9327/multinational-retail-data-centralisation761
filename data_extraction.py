#%%
import pandas as pd
import tabula
import requests
import concurrent.futures
import boto3
#%%
class DataExtractor:

    def __init__(self, database_connector = None, num_stores_endpoint=None, store_endpoint=None, header_dict=None, datetime_endpoint=None):
        self.database_connector = database_connector
        self.header_dict = header_dict
        self.num_stores_endpoint = num_stores_endpoint
        self.store_endpoint = store_endpoint
        self.datetime_endpoint = datetime_endpoint

    def read_rds_table(self, table_name):
        '''extract database table'''
        table_df = pd.read_sql_table(table_name=table_name, con=self.database_connector.init_db_engine())
        return table_df
    
    def retrieve_pdf_data(self, url):
        df = tabula.read_pdf(url, pages='all')
        merged_df = pd.concat(df)
        merged_df.reset_index(drop=True, inplace=True)
        return merged_df
    
    def list_number_of_stores(self):
        no_stores_response = requests.get(self.num_stores_endpoint, headers=self.header_dict)
        return int(no_stores_response.json()['number_stores'])

    def retrieve_stores_data(self):
        '''The two endpoints for the API are as follows:
            Retrieve a store: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}
            Return the number of stores: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores
            API key for this example: yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'''
        
        def fetch_url(url_and_header_dict):
            url, header_dict = url_and_header_dict
            response = requests.get(url, headers=header_dict)
            store_info = response.json()
            return store_info

        urls = [f"{self.store_endpoint}{store_number}" for store_number in range(self.list_number_of_stores())]
        args = [(url, self.header_dict) for url in urls]
        store_info_dict = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            store_info = executor.map(fetch_url, args)
            for i, store_info in enumerate(store_info):
                    store_info_dict[i] = store_info

        store_info_df = pd.DataFrame.from_dict(store_info_dict)
        store_info_df = store_info_df.T
        store_info_df.rename(columns={'index': 'store_number'}, inplace=True)
        
        return store_info_df
    
    def retrieve_datetime_data(self):

        datetime_response = requests.get(self.datetime_endpoint, json=True)
        datetime_dict = datetime_response.json()
        datetime_df = pd.DataFrame(datetime_dict)
        datetime_df.index = datetime_df.index.astype(int)
        return datetime_df


    @staticmethod
    def extract_from_s3(s3_address):
        '''For use with e.g. s3://data-handling-public/products.csv'''
        s3 = boto3.client('s3')
        s3_address_split = s3_address.split('/')
        product_data_obj_key = s3_address_split[-1] # object key references what's inside the S3
        bucket_name = s3_address_split[-2]
        s3.download_file(bucket_name, product_data_obj_key, 'products.csv')
        products_df = pd.read_csv('products.csv', index_col='Unnamed: 0')
        return products_df
    
