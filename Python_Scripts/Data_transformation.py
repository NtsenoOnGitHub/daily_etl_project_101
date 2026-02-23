import pandas as pd
import numpy as np
from write_to_log import log
import datetime
import json
import boto3
import datetime


def load_json_from_s3(bucket_name, object_key):
    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=bucket_name,
        Key=object_key
    )

    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)

    return data

bucket = "etl-project-s3-bucket-ntseno-2026"
key = f"data/raw_data_{datetime.date.today()}.json"

json_data = load_json_from_s3(bucket, key)

# Transfrom the Json data to semi-structured csv formate
normalized_data = pd.json_normalize(json_data['data'])
transformed_data =  pd.DataFrame(normalized_data)

transformed_data.to_csv(f'data/structured_data_{datetime.date.today()}.csv', index=False)
