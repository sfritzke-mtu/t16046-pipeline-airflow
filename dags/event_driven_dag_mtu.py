import json
import io
import pandas as pd
import os

import sys
sys.path.append("/opt/airflow/dags/mtu")
sys.path.append("/opt/airflow/dags/mtu/converter")

from dags.mtu.converter import convert_to_df
from dags.mtu.transformer import process_df


#import src 
#from version_detector import check_version

#conda create -n "airflow-kafka-env" python=3.12
#conda activate airflow-kafka-env
#conda info --envs

#docker exec kafka bash -c   "kafka-topics --list --bootstrap-server localhost:29092"
#docker rm -f $(sudo docker ps -a -q)
#docker image remove -f $(sudo docker images -a -q)

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, Asset, AssetWatcher, task


##mydf = pd.DataFrame()
## Globale Variablen funktionieren nicht

import boto3
from elasticsearch import Elasticsearch
import io

#and then you can use boto3 methods for manipulating buckets and files
#for example:

INDEX_NAME = "t16046_prod_data_idx"


def apply_function(*args,**kwargs):
    message = args[-1]
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val


trigger = MessageQueueTrigger(
    queue="kafka://localhost:9092/my_topic",
    #queue="kafka://localhost:29092/my_topic",
    apply_function="dags.event_driven_dag.apply_function",
)

kafka_topic_asset = Asset(
    "kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)]
)


@dag(schedule=[kafka_topic_asset])
def event_driven_dag_mtu():

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
    def process_parquet_files(bucket_intermediate: str, bucket_prod: str, prefix: str):
        print("Process parquet files and write results to s3 Bucket --> "+ bucket_prod + "/" + prefix)

        s3 = boto3.client('s3',
                            endpoint_url="http://minio:9000",
                            aws_access_key_id= "admin",
                            aws_secret_access_key= "minio-password"                         
                          )
        
        print("Read parquet-Objects in " + bucket_intermediate + "/" + prefix)
        response = s3.list_objects_v2(Bucket=bucket_intermediate, Prefix =prefix)
        for obj in response['Contents']:                
                print("Process Parquet file " + obj["Key"])                

                # Read the parquet file
                parquet_obj = s3.get_object(Bucket = bucket_intermediate, Key = obj["Key"])
                df = pd.read_parquet(io.BytesIO(parquet_obj['Body'].read()))

                print(df["serialn"].unique()[0]) 
                print(df["cycle"]) 
                
                process_df.setDuration(df)

                print(df["process_duration"])

                print("Write output to " + bucket_prod)
                out_buffer = io.BytesIO()
                df.to_parquet(out_buffer, index=False)
                s3.put_object(Bucket=bucket_prod, Key=obj["Key"], Body=out_buffer.getvalue())
                
                # #Alle Dateien als Ergebnis in prod kopieren
                # print("Write " + obj['Key'] + " to " + bucket_prod +"/"+prefix)
                # source = {'Bucket': bucket_intermediate, 'Key': obj['Key']}
                # # obj-Key enthält schon das Prefix
                # dest = s3_ressource.Bucket(bucket_prod)
                # # Falls Datei im Ziel vorhanden, wird diese überschrieben
                # dest.copy(source, obj['Key'])


    ##Elasticsearch funktioniert zur Zeit nicht mit Airflow 3
    @task
    def index_files_in_elasticsearch(bucket_prod: str, prefix: str):
        print("Index prod-data in elasticsearch")

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


        es = Elasticsearch("http://elasticsearch:9200")   

        response = s3.list_objects_v2(Bucket=bucket_prod, Prefix =prefix)
        for obj in response['Contents']:

            # Read the parquet file
            parquet_obj = s3.get_object(Bucket = bucket_prod, Key = obj["Key"])
            df = pd.read_parquet(io.BytesIO(parquet_obj['Body'].read()))


            # Index anlegen, falls es noch nicht gibt   
            if not es.indices.exists(index=INDEX_NAME):
                print("Create new index " + INDEX_NAME)
                es.indices.create(index=INDEX_NAME)   

            # Falls document noch nicht vorhanden...
            if not es.exists(index = INDEX_NAME, id = obj["Key"]):
                #Bucketname/ + filename
                print("Index new Document " + obj["Key"])

                ## Index new Document
                document = {
                    "maschine" : df["maschine"].unique()[0], #111213  
                    "materialnr" : df["material"].unique()[0], #456789
                    "avo" : df["operation"].unique()[0], #0090 
                    "serialnr" : df["serialn"].unique()[0] #123456 
                }

                response = es.index(index=INDEX_NAME, body=document, id=obj["Key"])
                print(response['_id'] + " created")
            else:
                ## ggf. Dokument aktualisieren
                print("Document " + obj["Key"] + " exists")

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

                # Read the json file
                json_obj = s3.get_object(Bucket = bucket_raw, Key = obj["Key"])
                content = json_obj['Body']

                data = json.loads(content.read())

                df = convert_to_df.convert(data)

                #print(df["serialn"])

                filename = prefix + df["serialn"].unique()[0] + "_Block_" + df["block"].unique()[0] + "_Schaufel_" + df["schaufel"].unique()[0] + "_" + df["befehl"].unique()[0] + ".parquet"
                print(os.getcwd())
                print(type(filename))
                print(filename)

                out_buffer = io.BytesIO()
                df.to_parquet(out_buffer, index=False)
                s3.put_object(Bucket=bucket_intermediate, Key=filename, Body=out_buffer.getvalue())
               
                #Als Beispiel alle Dateien als Ergebnis in intermediate kopieren
                #print("Write " + obj['Key'] + " to " + bucket_intermediate +"/"+prefix)
                #source = {'Bucket': bucket_raw, 'Key': obj['Key']}
                # obj-Key enthält schon das Prefix
                #dest = s3_ressource.Bucket(bucket_intermediate)
                # Falls Datei im Ziel vorhanden, wird diese überschrieben
                #dest.copy(source, obj['Key'])
          

    msg_dic = process_message()
    prefix =  str(msg_dic["Blisk"]) + "/"+ str(msg_dic["Bearbeitung"]) + "/"

    merge = merge_jsons_and_write_to_s3(bucket_raw = "t16046-raw-data", bucket_intermediate="t16046-intermediate-data", prefix=prefix)

    #Reihenfolge der Tasks festlegen
    msg_dic >> check_materialnr(str(msg_dic["Material-Nr"])) >> [merge, send_email()]                              
    merge >> process_parquet_files("t16046-intermediate-data", "t16046-prod-data", prefix = prefix)  >> index_files_in_elasticsearch("t16046-prod-data", prefix)
    

event_driven_dag_mtu()