
# Multinational Retail Data Centralisation

## Description

This project aims to centralize and streamline the sales data management for a multinational company. The primary objective is to consolidate various data sources into a single, accessible database, enabling more efficient data analysis and decision-making. The project involves data extraction from diverse sources such as AWS RDS databases, PDF documents, APIs, and S3 buckets, followed by data cleaning and integration into a PostgreSQL database.

Key learnings from this project so far:
- Implementing data extraction from various sources using Python.
- Handling and cleaning different data formats.
- Utilizing database operations for storing and managing large datasets.
- Developing a scalable and modular codebase that can be extended for additional data sources.

## Installation

To set up this project, follow these steps:

1. Clone the repository:
   ```
   git clone https://github.com/migs9327/multinational-retail-data-centralisation761
   ```
2. Install the required Python packages:
   ```
   pip install pandas sqlalchemy pyyaml tabula-py requests boto3
   ```
3. Set up a PostgreSQL database and update the `db_creds.yaml` with the appropriate credentials.

## Usage

To use the project, perform the following steps:

1. **Data Extraction**: Use the `DataExtractor` class to extract data from different sources. Modify the class methods based on your specific data sources.
   
2. **Data Cleaning**: Utilize the `DataCleaning` class to clean the extracted data. This includes handling missing values, correcting data types, and formatting issues.

3. **Database Operations**: Use the `DatabaseConnector` class to connect to the PostgreSQL database, list available tables, and upload cleaned data.

4. **Running Scripts**: Execute the scripts in the following order:
   - `data_extraction.py`
   - `data_cleaning.py`
   - `database_utils.py`

## File Structure

- `data_extraction.py`: Contains the `DataExtractor` class for data extraction from various sources.
- `data_cleaning.py`: Includes the `DataCleaning` class with methods to clean different types of data.
- `database_utils.py`: Comprises the `DatabaseConnector` class for database connection and operations.
- `db_creds.yaml`: Stores the database credentials (ensure this is secured and not tracked in version control).

## License

## License

MIT License

Copyright (c) 2023 Miguel Nava Harris

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

