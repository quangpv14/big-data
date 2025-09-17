from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round, rank, desc
from pyspark.sql.window import Window
from save_elasticsearch import write_batch_to_es
import logging

# Preprocess the stock DataFrame for analysis and visualization.
def prepare_stock_df(df):

    # 1️⃣ Feature engineering
    df = df.withColumn("@timestamp", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
    
    # Daily change and daily percentage change
    df = df.withColumn("daily_change", col("close") - col("open"))
    
    # Avoid division by zero
    df = df.withColumn("daily_pct_change", round((col("close") - col("open")) / col("open") * 100, 2))
    
    # Extract date only for daily ranking 
    df = df.withColumn("date_only", col("@timestamp").cast("date"))

    # 2️⃣ Rank tickers by daily percentage change per day
    w_day = Window.partitionBy("date_only").orderBy(desc("daily_pct_change"))
    df = df.withColumn("rank_in_day", rank().over(w_day))
    
    #df = df.repartition("ticker").cache()
    return df

