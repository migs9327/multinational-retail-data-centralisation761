#%%
import numpy as np
import pandas as pd
from dateutil.parser import parse
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import re
#%%
def safe_parse(d):
    '''Implements similar behaviour to errors=coerce in pd.to_datetime but for use with parse'''
    try:
        return parse(d)
    except Exception:
        return pd.NaT
    
def clean_datetime(value):
    safe_parsed = safe_parse(value)
    cleaned = pd.to_datetime(safe_parsed)
    return cleaned

#%%
class DataCleaning:

    def __init__(self, userdata_df=None, card_data_df=None, store_data_df=None, product_df=None, orders_df=None, datetime_df=None):
        self.userdata_df = userdata_df
        self.card_data_df = card_data_df
        self.store_data_df = store_data_df
        self.product_df = product_df
        self.orders_df = orders_df
        self.datetime_df = datetime_df

    def clean_user_data(self):
        self.userdata_df.replace('NULL', np.nan, inplace=True) 
        self.userdata_df[['date_of_birth', 'join_date']] = self.userdata_df[['date_of_birth', 'join_date']].applymap(safe_parse)
        self.userdata_df.dropna(inplace=True) # drops NaN rows and rows with unrecognised datetime formats
        self.userdata_df[['date_of_birth', 'join_date']] = self.userdata_df[['date_of_birth', 'join_date']].apply(pd.to_datetime, infer_datetime_format=True)
        self.userdata_df['country_code'].replace('GGB', 'GB', inplace=True)
        mask = self.userdata_df['join_date'] < self.userdata_df['date_of_birth'] # Filtering for impossible dob/join date combos
        self.userdata_df.loc[mask, 'date_of_birth'] = pd.NaT

        return self.userdata_df
    
    def clean_card_data(self):
        self.card_data_df.replace('NULL', np.nan, inplace=True)
        expiry_date_format = "%m/%y" # lowercase for 2-digit
        self.card_data_df['expiry_date'] = self.card_data_df['expiry_date'].apply(pd.to_datetime, format=expiry_date_format, errors='coerce')
        self.card_data_df['date_payment_confirmed'] = self.card_data_df['date_payment_confirmed'].apply(safe_parse)
        self.card_data_df['date_payment_confirmed'] = self.card_data_df['date_payment_confirmed'].apply(pd.to_datetime)
        self.card_data_df.dropna(inplace=True)

        return self.card_data_df
    
    def clean_store_data(self):

        df = self.store_data_df
        df.replace(['NULL', 'N/A', None], np.nan, inplace=True)
        df.drop('lat', axis=1, inplace=True)
        columns_to_numeric = ['longitude', 'staff_numbers', 'latitude']
        df[columns_to_numeric] = df[columns_to_numeric].apply(pd.to_numeric, errors='coerce')
        nonsense_rows_index = df['locality'].dropna()[~df['locality'].dropna().str.match(r'^[A-Za-z\s-]+$')].index
        all_nan_rows_index = df.iloc[:,1:][df.iloc[:,1:].isna().all(axis=1)].index # excludes store_number from the search for all NaN rows
        df.drop(nonsense_rows_index.union(all_nan_rows_index), inplace=True)
        df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'}, inplace=True)
        df['opening_date'] = df['opening_date'].apply(pd.to_datetime, infer_datetime_format=True)

        return df
    
    def convert_product_weights(self):

        self.product_df.dropna(inplace=True)
        # Split weight values from units
        weight_lists = self.product_df['weight'].apply(lambda x: re.findall(r'\d+\.?\d*|\D+', x))
        weight_units = weight_lists.apply(lambda x: x[-1])
        weight_values = weight_lists.apply(lambda x: x[:-1])
        # Handle multipacks by totalling their weights
        multipack_weights = weight_values[weight_values.apply(lambda x: len(x) == 3)]
        multipack_weights = multipack_weights.apply(lambda x: float(x[0]) * float(x[-1]))
        weight_values.update(multipack_weights)
        indices_to_drop = weight_values[weight_values.apply(lambda x: len(x) > 1 if type(x) == list else False)].index
        weight_values.drop(indices_to_drop, inplace=True)
        weight_units.drop(indices_to_drop, inplace=True)
        self.product_df.drop(indices_to_drop, inplace=True)
        # Extract the manipulated weights
        weight_values = weight_values.apply(lambda x: float(x[0]) if type(x) == list else x)
        # Clean erroneous values
        weight_units.replace(['JTL', 'ZTDGUZVU', 'RYSHX'], np.NaN, inplace=True)
        weight_units.replace('g .', 'g', inplace=True)
        self.product_df['weight_values'], self.product_df['weight_units'] = weight_values, weight_units
        # Convert to kg
        def convert_weight_values(weight_value, weight_unit):
            if weight_unit == 'kg':
                return weight_value
            elif weight_unit == 'g' or 'ml':
                return weight_value / 1000
            elif weight_unit == 'oz':
                return weight_value / 35.274
            else:
                raise ValueError('Unknown weight unit')
            
        self.product_df['weight/kg'] = self.product_df.apply(lambda row: convert_weight_values(row['weight_values'], row['weight_units']), axis=1)
        return self.product_df

    def clean_products_data(self):

        # Convert date values
        self.product_df['date_added'] = self.product_df['date_added'].apply(safe_parse)
        self.product_df['date_added'] = self.product_df['date_added'].apply(pd.to_datetime)
        # Extract price values
        pricevalue_pattern = r'(\d+\.?\d+)' # Match numeric parts of price string
        self.product_df['product_price'] = self.product_df['product_price'].str.extract(pricevalue_pattern)
        self.product_df.rename(columns={'product_price': 'product_price/£'}, inplace=True)
        pd.to_numeric(self.product_df['product_price/£'])
        self.product_df.drop(['weight', 'weight_values', 'weight_units'], axis=1, inplace=True)
        return self.product_df
    
    def clean_orders_data(self):
        self.orders_df.drop(['level_0', 'index', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        return orders_df
    
    def clean_datetime_data(self):       

        self.datetime_df.replace('NULL', np.NaN, inplace=True)
        timestamp_df = self.datetime_df[['timestamp', 'month', 'year', 'day']]
        timestamp_df['combined'] = timestamp_df['year'].astype(str) + '-' + timestamp_df['month'].astype(str) + '-' + timestamp_df['day'].astype(str) + ' ' + timestamp_df['timestamp'].astype(str)
        timestamp_df['combined'] = timestamp_df['combined'].apply(clean_datetime)
        self.datetime_df['timestamp'] = timestamp_df['combined']
        self.datetime_df.dropna(inplace=True)
        return datetime_df


#%%
my_db_creds_path = r"C:\Users\migue\Documents\AiCore\multinational-retail-data-centralisation761\salesdata_db_creds.yaml"
aicore_db_creds_path = r"C:\Users\migue\Documents\AiCore\multinational-retail-data-centralisation761\db_creds.yaml"
#%%
# Extract, clean and upload datetime data
datetime_endpoint = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
de = DataExtractor(datetime_endpoint=datetime_endpoint)
datetime_df = de.retrieve_datetime_data()
cleaner = DataCleaning(datetime_df=datetime_df)
clean_datetime_data = cleaner.clean_datetime_data()
datetime_data_dbcon = DatabaseConnector(db_creds_path=my_db_creds_path)
datetime_data_dbcon.upload_to_db(clean_datetime_data, 'dim_date_times')
#%%
# Extract, clean and upload orders data
orders_dbcon = DatabaseConnector(db_creds_path=aicore_db_creds_path)
de = DataExtractor(orders_dbcon)
orders_df = de.read_rds_table('orders_table')
cleaner = DataCleaning(orders_df=orders_df)
clean_orders_data = cleaner.clean_orders_data()
orders_data_dbcon = DatabaseConnector(db_creds_path=my_db_creds_path)
orders_data_dbcon.upload_to_db(clean_orders_data, 'orders_table')
#%%
# Extract, clean and upload product data
product_df = DataExtractor.extract_from_s3('s3://data-handling-public/products.csv')
cleaner = DataCleaning(product_df=product_df)
cleaner.convert_product_weights()
clean_product_data = cleaner.clean_products_data()
product_data_dbcon = DatabaseConnector(db_creds_path=my_db_creds_path)
product_data_dbcon.upload_to_db(clean_product_data, 'dim_products')

#%%
# Extract, clean and upload card data
de = DataExtractor()
card_data_df = de.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
cleaner = DataCleaning(card_data_df=card_data_df)
clean_card_data = cleaner.clean_card_data()
sales_data_dbcon = DatabaseConnector(db_creds_path=my_db_creds_path)
sales_data_dbcon.upload_to_db(clean_card_data, 'dim_card_details')

# %%
# Cleaning and uploading store data
header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX', "Content-Type": "application/json"}
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
de = DataExtractor(header_dict=header_dict, num_stores_endpoint=num_stores_endpoint, store_endpoint=store_endpoint)
store_data_df = de.retrieve_stores_data()
# %%
cleaner = DataCleaning(store_data_df=store_data_df)
clean_store_data = cleaner.clean_store_data()
# %%
sales_data_dbcon = DatabaseConnector(db_creds_path=my_db_creds_path)
sales_data_dbcon.upload_to_db(clean_store_data, 'dim_store_details')
