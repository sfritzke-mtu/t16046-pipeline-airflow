import json
import boto3

from airflow.sdk import dag, task


MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "minio-password"
BUCKET_NAME = "t16046-raw-data"

@dag
def upload_test_json_dag():

    @task
    def upload_json_to_s3():
        # Initialize MinIO Client
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )

        file = "test.json"
        obj_status = s3.list_objects(Bucket = BUCKET_NAME, Prefix = "LENCBH4711/Schlichten/" + file)
        
        if not obj_status.get("Contents"):
            #Upload file Upload file to MinIO

            with open("/usr/local/airflow/dags/data/test.json") as file_obj:
                data = json.load(file_obj)            


            s3.put_object(Body=json.dumps(data), Bucket=BUCKET_NAME, Key="LENCBH4711/Schlichten/" + file)
            print(f"Uploaded {file} to bucket {BUCKET_NAME}")

        else:
            print(f"Object {file} exists ")

    @task
    def download_json_from_s3():
        # Initialize MinIO Client
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )

        response = s3.get_object(Bucket = BUCKET_NAME, Key = "LENCBH4711/Schlichten/test.json" )
        content = response['Body']

        data = json.loads(content.read())
        print(data)                


    upload_json_to_s3() >> download_json_from_s3()
    
upload_test_json_dag()