from kafka import KafkaProducer
import json
import boto3
import os

# pip install kafka-python

# Kafka Configuration
#https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/
#KAFKA_BROKER = "localhost:19092"  # If running from the host
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
    aws_secret_access_key=MINIO_SECRET_KEY
)


for subdir, dirs, files in os.walk(DATA):
    for file in files:
        filepath = subdir + os.sep + file        

        if (filepath.endswith(".json") and not filepath.endswith("test.json")):

            obj_status = s3.list_objects(Bucket = BUCKET_NAME, Prefix = "LENCBH4711/Schlichten/" + file)
            
            if not obj_status.get("Contents"):
                #Upload file Upload file to MinIO
                #s3.upload_file(filepath, BUCKET_NAME, "LENCBH4711/Schlichten/" + file)
                with open(filepath) as file_obj:
                    data = json.load(file_obj)            


                s3.put_object(Body=json.dumps(data), Bucket=BUCKET_NAME, Key="LENCBH4711/Schlichten/" + file)

                print(f"Uploaded {file} to bucket {BUCKET_NAME}")

            else:
                print(f"Object {file} exists ")
            

print("All files uploated to S3 Storage")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(7,6,5)
)

#Notify always Kafka
message = {"event": "T16046 Datenpipeline with Airflow",
        "Blisk" :"LENCBH4711",
        "Material-Nr" : "32A4302",
        "Bearbeitung" : "Schlichten"
        }

producer.send(TOPIC_NAME, value=message)
producer.flush()
print(f"Message sented to Kafka: {message}")




