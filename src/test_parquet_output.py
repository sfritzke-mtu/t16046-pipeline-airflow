
import boto3
import io
import pandas as pd

s3 = boto3.client('s3',
                    endpoint_url="http://localhost:9000",
                    aws_access_key_id= "admin",
                    aws_secret_access_key= "minio-password"                         
                    )

PROD_BUCKET = "t16046-prod-data"

print("Read parquet-Objects in " + PROD_BUCKET)
# response = s3.list_objects_v2(Bucket=PROD_BUCKET, Prefix = "LENCBH4711/Schlichten/")
# for obj in response['Contents']:                
#         print("Process Parquet file " + obj["Key"])                

#         # Read the parquet file
#         parquet_obj = s3.get_object(Bucket = PROD_BUCKET, Key = obj["Key"])
#         df = pd.read_parquet(io.BytesIO(parquet_obj['Body'].read()))

#         print(df["serialn"].unique()[0]) 
#         print(df["process_duration"]) 
        


s3_ressource = boto3.resource('s3',
                        endpoint_url="http://localhost:9000",
                        aws_access_key_id="admin",
                        aws_secret_access_key="minio-password"                          
                        )

bucket = s3_ressource.Bucket(PROD_BUCKET)
for obj in bucket.objects.all():
    key = obj.key
    print("Read file " + key)
    df_prod = pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))
    print(df_prod["serialn"].unique()[0]) 
    print(df_prod["process_duration"]) 
