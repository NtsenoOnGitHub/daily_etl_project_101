import psycopg2
import os
import datetime
from dotenv import load_dotenv
from write_to_log import log
from s3_file_upload import upload_to_s3

AWS_DB_HOST = os.getenv('AWS_DB_HOST')
AWS_DB_PORT = os.getenv('AWS_DB_PORT')
AWS_DB_NAME = os.getenv('AWS_DB_NAME')
AWS_DB_USER = os.getenv('AWS_DB_USER')
AWS_DB_PASSWORD = os.getenv('AWS_DB_PASSWORD')

SQL_CREATE_TABLE = """CREATE TABLE IF NOT EXIST insder_transactions (
                            transaction_date TIMESTAMP,
                            ticker VARCHAR(50),
                            executive VARCHAR(50),
                            executive_title VARCHAR(50),
                            security_type VARCHAR(50),
                            acquisition_or_disposal VARCHAR(50),
                            shares VARCHAR(50),
                            share_price VARCHAR(50)

)"""

def db_call(query: str):
    try:
        connection = psycopg2.connect(
            host=AWS_DB_HOST,
            port=AWS_DB_PORT,
            dbname=AWS_DB_NAME,
            user=AWS_DB_USER,
            password=AWS_DB_PASSWORD
        )

        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()

        cursor.close()
        connection.close()

        log_message = f"Message {datetime.datetime.now()}: Query executed successfully\n"
        log(log_message)

    except Exception as e:
        log_message = f"Error {datetime.datetime.now()}: Database connection failed: {e}\n"
        log(log_message)
        raise



db_call(SQL_CREATE_TABLE)

upload_to_s3('logs/program.log', 'etl-project-s3-bucket-ntseno-2026', 'logs/program.log')











