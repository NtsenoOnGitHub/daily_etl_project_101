import pandas as pd
import requests
import os
import datetime
from dotenv import load_dotenv  # This is only applicable for running the script locally.
from write_to_log import log
import os
import json
import boto3

# load_dotenv('config/.env') # This is only applicable for running the script locally.
MY_API_KEY = os.getenv('API_KEY')

# - Extract data from Alpha Vantage using API call
# - APIKEY is your secret api key from your Alpha Vantage account
# - This will return a data in a json format

def extract_data(API: str):
    try:
        url = f'https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol=IBM&date=2017-11-15&apikey=demo' #{API}
        r = requests.get(url)
        data = r.json()
        return data
    except:
        return 'Something went wrong' # replace this with logs functionality
# - 
# - The returned json file will be saved in AWS S3 bucket
def upload_to_s3(file_name: str, bucket_name: str, file_path):
    s3_client = boto3.client("s3",region_name="af-south-1")
    try:
        s3_client.upload_file(file_name, bucket_name, file_path)
        log_message = f"Successfully uploaded {file_name} to {bucket_name}/{file_path}\n"
        log(log_message)
    except Exception as e:
        log_message = f"Error uploading file: {e}\n"
        log(log_message)

json_data = extract_data(MY_API_KEY)
    
if json_data is None:
    log_message = 'The API call failed'
    log(log_message)
elif isinstance(json_data, dict) or isinstance(json_data, list):
    log_message = f'Message {datetime.datetime.now()}: Data has been extracted successfuly from a Alpha Vantage on an API call\n'
    log(log_message)
    with open(f'data/raw_data_{datetime.date.today()}.json', 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)

    # Upload the json file to s3 bucket
    upload_to_s3(f'data/raw_data_{datetime.date.today()}.json', 
                 'etl-project-s3-bucket-ntseno-2026', 
                 f'data/raw_data_{datetime.date.today()}.json')
    # Upload the log file to aws s3 bucket 
    upload_to_s3('logs/program.log', 
                 'etl-project-s3-bucket-ntseno-2026', 
                 'logs/program.log')