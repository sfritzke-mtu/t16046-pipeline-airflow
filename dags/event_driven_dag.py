import json
import pandas as pd
import os

#conda create -n "airflow-kafka-env" python=3.12
#conda activate airflow-kafka-env
#conda info --envs

""" {
  "aws_access_key_id": "admin",
  "aws_secret_access_key": "minio-password",
  "host": "http://minio:9000"
} """

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, Asset, AssetWatcher, task


##mydf = pd.DataFrame()
## Globale Variablen funktionieren nicht

import boto3


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
    def process_message(**context):
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
    def process_parquet_files(prefix_intermediate: str, prefix_prod: str):
        print("Write parquet-file to s3 Bucket --> "+ prefix_prod)

    @task
    def index_files_in_elasticsearch():
        print("Write parquet-file")

    @task
    def merge_jsons_and_write_to_s3(bucket_raw: str, bucket_intermediate: str, prefix: str):

        s3 = boto3.client('s3',
                            endpoint_url="http://minio:9000",
                            aws_access_key_id= "admin",
                            aws_secret_access_key= "minio-password"                         
                          )
        
        s3_ressource = boto3.resource('s3',
                            endpoint_url="http://minio:9000",
                            aws_access_key_id="admin",
                            aws_secret_access_key="minio-password"                          
                          )

        print("Read JSON-Objects in " + bucket_raw + "/" + prefix)
        response = s3.list_objects_v2(Bucket=bucket_raw, Prefix =prefix)
        for obj in response['Contents']:                
                print("Merge JSON files and write to intermediate-bucket")
                #Hier die Merge-Funktion aufrufen
                
                #Als Beispiel alle Dateien als Ergebnis in intermediate kopieren
                print("Write " + obj['Key'] + " to " + bucket_intermediate +"/"+prefix)
                source = {'Bucket': bucket_raw, 'Key': obj['Key']}
                # obj-Key enthält schon das Prefix
                dest = s3_ressource.Bucket(bucket_intermediate)
                # Falls Datei im Ziel vorhanden, wird diese überschrieben
                dest.copy(source, obj['Key'])
          

    msg_dic = process_message()
    prefix =  str(msg_dic["Blisk"]) + "/"+ str(msg_dic["Bearbeitung"]) + "/"

    merge = merge_jsons_and_write_to_s3(bucket_raw = "t16046-raw-data", bucket_intermediate="t16046-intermediate-data", prefix=prefix)

    #Reihenfolge der Tasks festlegen
    msg_dic >> check_materialnr(str(msg_dic["Material-Nr"])) >> [merge, send_email()]                              
    # merge >> process_parquet_files(s3_intermediate_prefix, s3_prod_prefix) >> index_files_in_elasticsearch()
    

event_driven_dag()