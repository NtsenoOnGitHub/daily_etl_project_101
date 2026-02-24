import pandas as pd
import requests
import os
import datetime
from write_to_log import log
import json
import boto3

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
