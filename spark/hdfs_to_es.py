from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round, rank, desc, lit
from pyspark.sql.window import Window
from save_elasticsearch import write_batch_to_es
import logging
from datetime import datetime, timedelta

def get_last_week_range():
    today = datetime.now()
    start_date_this_week = today - timedelta(days=today.weekday())   
    start_date_last_week = start_date_this_week - timedelta(days=7)  
    end_date_last_week   = start_date_last_week + timedelta(days=5)

    return (
        start_date_last_week.strftime('%Y-%m-%d'),
        end_date_last_week.strftime('%Y-%m-%d')
    )
            
# Process the stock DataFrame for analysis and visualization.
def process_stock_df(df):

    print("=== Sample data from HDFS ===")
            
    # Count record before deduplication
    print("=== Before: Total data count (dedup):", df.count(), "===")
    
    print("=== Record count per ticker (raw) ===")
    df.groupBy("ticker").count().orderBy("ticker").show(50, truncate=False)

    # Deduplicate
    df = df.dropDuplicates(["ticker", "time"])
    
    # 1️⃣ Feature engineering
    df = df.withColumn("@timestamp", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
    
    # Daily change and daily percentage change
    df = df.withColumn("daily_change", col("close") - col("open"))
    
    # Avoid division by zero
    df = df.withColumn("daily_pct_change", round((col("close") - col("open")) / col("open") * 100, 2))
    
    # Extract date only for daily ranking 
    df = df.withColumn("date_only", col("@timestamp").cast("date"))
    
    # Clean data
    clean_df = clean_data(df)

    # 2️⃣ Rank tickers by daily percentage change per day
    w_day = Window.partitionBy("date_only").orderBy(desc("daily_pct_change"))
    clean_df = clean_df.withColumn("rank_in_day", rank().over(w_day))
    
    return clean_df

def clean_data(df):
    start_date_str, end_date_str = get_last_week_range()
    clean_df = df.filter(
        (df['date_only'] >= lit(start_date_str)) & 
        (df['date_only'] <= lit(end_date_str))
    )
    
    # Count records after deduplication
    print("=== Clean data count (dedup):", clean_df.count(), "===")
    
    print("=== Record count per ticker (clean) ===")
    clean_df.groupBy("ticker").count().orderBy("ticker").show(50, truncate=False)
                
    # Sample data ACB
    print("=== Sample data ACB ===")
    clean_df.filter(clean_df['ticker'] == 'ACB').show()
    
    return clean_df

