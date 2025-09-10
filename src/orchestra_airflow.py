from kafka import KafkaProducer
import json
import boto3
import os

# pip install kafka-python

# Kafka Configuration
#KAFKA_BROKER = "localhost:9092"  # If running from the host
KAFKA_BROKER = "kafka:9092"  # If running inside Docker
TOPIC_NAME = "my_topic"

#Input-Ordner
DATA = "/home/digilab/t16046-pipeline-airflow/dags/data/"

# MinIO Configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "minio-password"
BUCKET_NAME = "t16046-raw-data"

# Initialize MinIO Client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


for subdir, dirs, files in os.walk(DATA):
    for file in files:
        filepath = subdir + os.sep + file        

        if filepath.endswith(".parquet"):

            obj_status = s3.list_objects(Bucket = BUCKET_NAME, Prefix = "LENCBH4177/Finish/" + file)
            
            if not obj_status.get("Contents"):
                #Upload file Upload file to MinIO
                s3.upload_file(filepath, BUCKET_NAME, "LENCBH4177/Finish/" + file)
                print(f"Uploaded {file} to bucket {BUCKET_NAME}")

            else:
                print(f"Object {file} exists ")
            

print("All files uploated to S3 Storage")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

#Notify always Kafka
message = {"event": "Test Apache with Airflow",
        "Blisk" :"LENCBH4177",
        "Material-Nr" : "32A4302",
        "Bearbeitung" : "Finish"
        }

producer.send(TOPIC_NAME, value=message)
producer.flush()
print(f"Message sented to Kafka: {message}")




