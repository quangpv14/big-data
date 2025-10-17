from elasticsearch import Elasticsearch
import json

# Kết nối tới Elastic Cloud 9.x
es = Elasticsearch(
    "https://83b2e7efa1ff42bb8f00002ab03ba09a.us-central1.gcp.cloud.es.io:443",
    basic_auth=("elastic", "0Mgek7P7cAHXlESQsmNo1QGD"),
    verify_certs=True
)

def is_trading_time():
    now = datetime.now() + timedelta(hours=7)

    wd = now.weekday()           # 0 = Monday … 6 = Sunday
    if wd > 4:                   # Saturday or Sunday
        return False

    hm = now.hour * 100 + now.minute
    return (915 <= hm <= 1130) or (1300 <= hm <= 1445)

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

