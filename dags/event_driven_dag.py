import json
import pandas as pd
import os

#conda create -n "airflow-kafka-env" python=3.12
#conda activate airflow-kafka-env
#conda info --envs

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, Asset, AssetWatcher, task
from airflow.sdk import Variable

##mydf = pd.DataFrame()
## Globale Variablen funktionieren nicht

import boto3
from airflow.hooks.base import BaseHook



#and then you can use boto3 methods for manipulating buckets and files
#for example:



def apply_function(*args,**kwargs):
    message = args[-1]
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val


trigger = MessageQueueTrigger(
    queue="kafka://localhost:9092/my_topic",
    apply_function="dags.event_driven_dag.apply_function",
)

kafka_topic_asset = Asset(
    "kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)]
)


@dag(schedule=[kafka_topic_asset])
def event_driven_dag():

    @task(multiple_outputs=True)
    def process_data(**context):
        # Extract the triggering asset events from the context
        triggering_asset_events = context["triggering_asset_events"]
        for event in triggering_asset_events[kafka_topic_asset]:
            # Get the message from the TriggerEvent payload
            print(f"Processing message: {event}")
            print(event.extra['payload']['event'])
            print(event.extra['payload']['Blisk'])

        return event.extra['payload']
        


    @task.branch
    def check_materialnr(materialnr : str):
        print("Überprüfen ob Material-Nr in der DB vorhanden ist, sonst E-Mail versenden")

        if materialnr == "32A4302":
           return "merge_jsons_and_write_to_s3"
        else:
          return "send_email"
    

    @task
    def send_email():
        print("Send E-Mail an Myriam")

    
    @task
    def merge_jsons_and_write_to_s3(prefix_raw: str, prefix_intermediate: str):
        #Input-Ordner aus dem Airflow-Container!!!
        DATA = "/usr/local/airflow/dags/data/"

        # MinIO Configuration
        MINIO_ENDPOINT = "http://host.docker.internal:9000"
        MINIO_ACCESS_KEY = "admin"
        MINIO_SECRET_KEY = "minio-password"
        BUCKET_NAME = "t16046-intermediate-data"

        # Initialize MinIO Client
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )        

        print("Merge jsons an write to s3 Bucket --> " + prefix_intermediate)

        for subdir, dirs, files in os.walk(DATA):
            for file in files:
                filepath = subdir + os.sep + file        

                if filepath.endswith(".parquet"):

                    obj_status = s3.list_objects(Bucket = BUCKET_NAME, Prefix = file)
                    print(obj_status)
                    
                    if not obj_status.get("Contents"):
                        #Upload file Upload file to MinIO
                        s3.upload_file(filepath, BUCKET_NAME, file)
                        print(f"Uploaded {file} to bucket {BUCKET_NAME}")

                        #df = pd.read_parquet(filepath, engine='pyarrow', columns =["schaufel", "serialn", "material", "operation", "block", "maschine"])
                    else:
                        print(f"Object {file} exists ")
                    
            print("All files uploated to S3 Storage")        

    @task
    def process_parquet_files(prefix_intermediate: str, prefix_prod: str):
        print("Write parquet-file to s3 Bucket --> "+ prefix_prod)

    @task
    def index_files_in_elasticsearch():
        print("Write parquet-file")

    @task
    def list_files():
        s3 = boto3.resource('s3',
                            endpoint_url="http://minio:9000",
                            aws_access_key_id="admin",
                            aws_secret_access_key="minio-password"
        )
        s3client = s3.meta.client 

        print("Objects in t16046-intermediate-data")
        bucket = s3.Bucket('t16046-intermediate-data')
                # Iterates through all the objects, doing the pagination for you. Each obj
                # is an ObjectSummary, so it doesn't contain the body. You'll need to call
                # get to get the whole body.
        for obj in bucket.objects.all():    
            key = obj.key
            print(key)    

        s3 = boto3.client('s3',
                            endpoint_url="http://minio:9000",
                            aws_access_key_id="admin",
                            aws_secret_access_key="minio-password"
                          
                          )

        print("Objects in t16046-raw-data/LENCBH4177/Finish")
        response = s3.list_objects_v2(Bucket='t16046-raw-data', Prefix ='LENCBH4177/Finish')
        for obj in response['Contents']:
                print(obj['Key'])
          

    # msg_dic = process_data()
    # s3_raw_prefix = "t16046-raw-data/" + str(msg_dic["Blisk"]) + "/"+ str(msg_dic["Bearbeitung"]) + "/"
    # s3_intermediate_prefix = "t16046-intermediate-data/" +  str(msg_dic["Blisk"]) + "/"+ str(msg_dic["Bearbeitung"]) + "/"
    # s3_prod_prefix = "t16046-prod-data/" +  str(msg_dic["Blisk"]) + "/"+ str(msg_dic["Bearbeitung"]) + "/"

    # merge = merge_jsons_and_write_to_s3(s3_raw_prefix, s3_intermediate_prefix)

    # msg_dic >> check_materialnr(str(msg_dic["Material-Nr"])) >> [merge, send_email()]                              
    # merge >> process_parquet_files(s3_intermediate_prefix, s3_prod_prefix) >> index_files_in_elasticsearch()
    
    list_files()

event_driven_dag()