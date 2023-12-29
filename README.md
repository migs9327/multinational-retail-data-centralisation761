
# Multinational Retail Data Centralisation

## Description

This project aims to centralize and streamline the sales data management for a multinational company. The primary objective is to consolidate various data sources into a single, accessible database, enabling more efficient data analysis and decision-making. The project involves data extraction from diverse sources such as AWS RDS databases, PDF documents, APIs, and S3 buckets, followed by data cleaning and integration into a PostgreSQL database.

## Key Learnings from This Project

- **Implementing Data Extraction from Various Sources Using Python**: Developed methods to efficiently extract data from AWS RDS databases, PDF documents, APIs, and S3 buckets using Python.
- **Handling and Cleaning Different Data Formats**: Applied techniques to clean and standardize data from diverse sources, ensuring data quality and consistency.
- **Utilizing Database Operations for Storing and Managing Large Datasets**: Gained proficiency in using PostgreSQL for large-scale data storage and management.
- **Developing a Scalable and Modular Codebase**: Created a codebase that can be easily extended for additional data sources and functionalities.
- **Database Schema Design, Optimization, and Implementation of a Star Schema for Data Warehousing**: Enhanced skills in designing and optimizing a database schema, including data type conversions and merging columns. Developed a star-based database schema for efficient data analysis, which involved linking various dimension tables (prefixed with `dim_`) to a central fact table (`orders_table`). This approach facilitated improved data integrity, efficient data storage and retrieval, and enabled more effective data analysis.
- **Advanced SQL Techniques**: Mastered advanced SQL operations, including data cleaning (like removing unwanted characters), creating human-readable categories based on data ranges, and handling NULL values.
- **Data Integrity and Relationships**: Enhanced understanding of data integrity by implementing primary and foreign key constraints, ensuring the reliability and accuracy of database relationships.
- **Unique Constraints and Data Cleaning in SQL**: Applied unique constraints for data consistency and performed additional data cleaning directly within SQL.

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

