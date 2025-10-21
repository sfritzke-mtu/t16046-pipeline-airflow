from airflow.sdk import dag, task
from elasticsearch import Elasticsearch


es = Elasticsearch("http://elasticsearch:9200")
INDEX_NAME = "t16046_prod_data_idx"


@dag
def index_dag():

    @task
    def index_new_document():
        # Index anlegen, falls es noch nicht gibt   
        if not  es.indices.exists(index=INDEX_NAME):
            print("Create new index " + INDEX_NAME)
            es.indices.create(index=INDEX_NAME)

            # Falls document noch nicht vorhanden...
            if not es.exists(index = INDEX_NAME, id = "test-sf"):
                #Bucketname/ + filename
                print("Index new Document test-sf")

                ## Index new Document
                document = {
                    "maschine" : "abc",
                    "materialnr" : "4711",
                    "avo" : "0200",
                    "serialnr" : "LENCBH4711",
                    "schaufelnr" : 1,
                    "block" : 2
                }

                response = es.index(index=INDEX_NAME, body=document, id="test-sf")
                print(response['_id'] + " created")

    @task
    def search_for_document():
        print("Search documents by serialnr = LENCBH4711 and schaufelnr = 1...")
        ## Query multiple fields
        query = {"query": {
        "bool": {
        "should": [
            {"match": {"serialnr": "LENCBH4711"}},
            {"match": {"schaufelnr": 1}}
                ]
                }
            }       
        }

        response = es.search(index=INDEX_NAME, body = query)
        #print(response)

        documents = response["hits"]["hits"]
        for doc in documents:
            print(f"Document ID: {doc['_id']} found!")         
            print(f"Document: {doc['_source']}")         

    index_new_document()
    search_for_document()

index_dag()