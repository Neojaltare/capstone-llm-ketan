import pytest
import os
import json 

import boto3

from capstonellm.common.catalog import llm_bucket

@pytest.fixture
def s3_path() -> str:
    destination = "cleaned/ketan/airflow" # example: cleaned/jonas/python-polars

    if not destination:
        raise Exception("TODO: specify the data destination in test_clean.py")

    return destination

def test_connection_aws():
    sts = boto3.client("sts")
    try:
        sts.get_caller_identity()
    except Exception as e:
        raise Exception(f"Failed to connect to AWS (check your credentials): {str(e)}")

def test_s3_path_exists(s3_path:str):
    s3 = boto3.client("s3")
    result = s3.list_objects(Bucket = llm_bucket, Prefix = s3_path)

    if "Contents" not in result:
        raise Exception(f"The path s3a//{llm_bucket}/{s3_path} does not exist")

    if not result.get("Contents"):
        raise Exception(f"Path  s3a//{llm_bucket}/{s3_path} exists but is empty")
    
    files = [file.get("Key") for file in result.get("Contents") if file.get("Key").endswith(".json")]
    
    if len(files) == 1:
        raise Exception(f"Path  s3a//{llm_bucket}/{s3_path} contains a single file")

def test_s3_file_format(s3_path:str):
    s3 = boto3.client("s3")
    result = s3.list_objects(Bucket = llm_bucket, Prefix = s3_path)

    if not result.get("Contents"):
        return  

    files = [file.get("Key") for file in result.get("Contents") if file.get("Key").endswith(".json")]

    if not files:
        return 

    data = json.loads(s3.get_object(Bucket=llm_bucket, Key=files[0])["Body"].read())

    required_keys = ['question_id', 'question', 'title', 'link', 'answer_id', 'answer']
    for key in required_keys:
        assert key in data, f"Result .json file misses required key {key}"