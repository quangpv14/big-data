
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, date_format, expr, element_at, to_timestamp, col, split
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, TimestampType, DoubleType, DateType, MapType
from save_elasticsearch import write_batch_to_es
from hdfs_to_es import prepare_stock_df
import time
import subprocess
import logging
import threading

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/user/root/kafka_data"
HDFS_CHECKPOINT_PATH = "hdfs://namenode:8020/user/root/checkpoints_hdfs"

def ensure_hdfs_path(path):
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", path], check=True)
        subprocess.run(["hdfs", "dfs", "-chmod", "-R", "777", path], check=True)
        print(f"HDFS path ready: {path}")
    except Exception as e:
        print(f"Failed to create HDFS path {path}: {e}")
        
def read_kafka_stream(spark, topic, schema, fail_on_data_loss=True):
    params = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": topic,
        "startingOffsets": "latest"
    }
    if fail_on_data_loss:
        params["failOnDataLoss"] = "false"
        
    kafka_df = spark.readStream.format("kafka").options(**params).load()
    print(f"Print data topic {topic}: ")
    kafka_df.printSchema()
    
    # Parse JSON => array<struct>
    kafka_str_df = kafka_df.selectExpr("CAST(value AS STRING)")
    stock_df = kafka_str_df.select(from_json(col("value"), schema).alias("data"))

    return stock_df.select(explode(col("data")).alias("stock_data")).select("stock_data.*")
    
def get_stock_historical_schema():
    return ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("time", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", IntegerType(), True)
    ]))

def get_realtime_schema():
    return ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("time", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True),
        StructField("match_type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("total_minutes", IntegerType(), True),
        StructField("prevPriceChange", DoubleType(), True)
    ]))
    
def jobStockHistoricalData(spark):
    # Read data from Kafka
    stock_df = read_kafka_stream(spark, "topic_stock_historical", get_stock_historical_schema())

    # Save to HDFS
    hdfs_save_query = stock_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", HDFS_OUTPUT_PATH) \
        .option("checkpointLocation", HDFS_CHECKPOINT_PATH) \
        .start()
      
    hdfs_save_query.awaitTermination()

def jobHdfsToESBatch(spark):

    while True:
        try:
            # Read data from HDFS
            df = spark.read.json(HDFS_OUTPUT_PATH)
            if df.head(1):
                print("=== Sample data from HDFS ===")
                df = prepare_stock_df(df)
                df.show(20, truncate=False)

                # Save to Elasticsearch
                write_batch_to_es(df, batch_id=int(time.time()), index_name="stock_historical")
            else:
                print("=== No data in HDFS to process ===")
        except Exception as e:
            print(f"Error processing HDFS to ES: {e}")
        
        # Sleep for a defined interval before next batch processing
        time.sleep(5)
        
def jobStockRealtimeData(spark):
    # Read data from Kafka
    stock_df = read_kafka_stream(spark, "topic_stock_realtime", get_realtime_schema())
    print("Print data realtime stock_df: ")
    
    # Console output
    query_console = stock_df.writeStream.outputMode("append").format("console").start()

    # push to Elasticsearch
    query_es = (
        stock_df.writeStream
        .foreachBatch(lambda df, batch_id: write_batch_to_es(df, batch_id, "stock_realtime"))
        .outputMode("append")
        .start()
    )
    
    # Await termination
    query_console.awaitTermination()
    query_es.awaitTermination()  

if __name__ == "__main__":
    # Define SparkSession
    spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Tạo và kiểm tra HDFS folder trước
    ensure_hdfs_path(HDFS_OUTPUT_PATH)
    ensure_hdfs_path(HDFS_CHECKPOINT_PATH)

    # Thử đọc dữ liệu HDFS nếu có
    try:
        df = spark.read.json(HDFS_OUTPUT_PATH)
        if df.head(1):
            print("=== Dữ liệu đọc từ HDFS ===")
            df.show(truncate=False)
        else:
            print("=== HDFS hiện không có dữ liệu ===")
    except Exception as e:
        print(f"=== Lỗi khi đọc HDFS: {e} ===")
        
    t1 = threading.Thread(target=jobStockHistoricalData, args=(spark,))
    t2 = threading.Thread(target=jobHdfsToESBatch, args=(spark,))
    t3 = threading.Thread(target=jobStockRealtimeData, args=(spark,))
    t1.start()
    t3.start()

    t1.join()
    t2.start()
    t2.join()
    
    t3.join()