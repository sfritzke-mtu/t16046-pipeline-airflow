from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "t16046_prod_data_idx"

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