from elasticsearch import Elasticsearch
import json

# Kết nối tới Elastic Cloud 9.x
es = Elasticsearch(
    "https://f7fedd4c7b1a415088149be42a425104.us-central1.gcp.cloud.es.io:443",
    basic_auth=("elastic", "ndYyNWgRX2fi2J1Vv7D8Lljq"),
    verify_certs=True
)

def write_batch_to_es(batch_df, batch_id, index_name):
    """
    Ghi từng micro-batch của Spark Structured Streaming
    vào Elasticsearch bằng Bulk API.
    """
    docs = batch_df.toJSON().collect()
    actions = []
    for doc in docs:
        actions.append({"index": {"_index": index_name}})
        actions.append(json.loads(doc))
    if actions:
        es.bulk(body=actions)

