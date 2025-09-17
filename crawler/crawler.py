from confluent_kafka import Producer, KafkaException, KafkaError
import json
import os
import pandas as pd
from vnstock import *
from datetime import datetime, timedelta
from time import sleep
import threading

vn = Vnstock()
# ============================== Kafka Config ==============================
BOOTSTRAP_SERVERS = "kafka:9092"                     # Address Kafka broker
KAFKA_TOPIC_HISTORICAL = "topic_stock_historical"    # Topic historical name
KAFKA_TOPIC_REALTIME = "topic_stock_realtime"        # Topic realtime name
# ==========================================================================

# Function to check Kafka connection
def check_kafka_connection(bootstrap_servers, timeout=10):
    try:
        producer = Producer({'bootstrap.servers': bootstrap_servers})
        # GET metadata to check connection
        metadata = producer.list_topics(timeout=timeout)
        print(f"[INFO] Connected to Kafka broker: {metadata.orig_broker_name}")
        return True
    except KafkaException as e:
        print(f"[ERROR] Cannot connect to Kafka: {e}")
        return False
    
# Function to send JSON data to Kafka with stock code
def send_to_kafka_json(bootstrap_servers, topic_name, symbol, json_message):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    producer.produce(
        topic_name, 
        value=json_message.encode('utf-8'), 
        key=symbol.encode('utf-8'), 
        callback=delivery_report
    )
    producer.flush()

#  Kafka producer callback function.
#  This will be called once for each message produced to indicate delivery result.
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered successfully: {}'.format(msg.key().decode('utf-8')))

def get_last_week_range():
    today = datetime.now()
    start_date_this_week = today - timedelta(days=today.weekday())
    start_date_last_week = start_date_this_week - timedelta(days=7)
    end_date_last_week   = start_date_last_week + timedelta(days=5)

    return (
        start_date_last_week.strftime('%Y-%m-%d'),
        end_date_last_week.strftime('%Y-%m-%d')
    )

# Function to fetch stock data for a specific symbol and period(start_date, end_date)
def get_stock_data_last_week(symbol, start_date, end_date):

    # Initialize Quote object
    quote = Quote(symbol=symbol)
    # Fetch historical data for the last week
    df = quote.history(start_date, end_date, interval='1D')

    # convert to JSON
    df["ticker"] = symbol
    df['open'] = df['open'] * 1000
    df['high'] = df['high'] * 1000
    df['low'] = df['low'] * 1000
    df['close'] = df['close'] * 1000
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else None)    
    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

def get_stock_data_intraday_realtime(symbol): 
    try:
        df = Quote(symbol=symbol).intraday(interval='1m') 
    except ValueError as e:
        print(f"[WARNING] Không thể lấy dữ liệu {symbol}: {e}")
        return None  # or return pd.DataFrame() ={}
    
    # Add ticker column
    df["ticker"] = symbol
    # Convert price from thousands of VND to VND
    df['price'] = df['price'] * 1000  
       
    # Process the 'time' column
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')
    # total_minutes = (hours * 60 + minutes) - (market open at 9:15)
    df['total_minutes'] = df['time'].dt.hour * 60 + df['time'].dt.minute - 9*60 -15
    
    # Process prevPriceChange column
    df['prevPriceChange'] = df['price'].diff().fillna(0)
    df['time'] = df['time'].dt.strftime('%H:%M:%S')
    df = df.drop_duplicates(subset=['total_minutes'])
    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

def crawl_vn30_historical_job(kafka_topic, bootstrap_servers):
    stock_array = ["ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG","MBB","MSN","MWG","PLX","POW","SAB","SHB","SSB","SSI","STB","TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE"]
    start_date_str, end_date_str = get_last_week_range()
    all_data={}

    # Get url of current script
    base_dir = os.path.dirname(os.path.abspath(__file__))  
    file_path = os.path.join(base_dir, "stock_historical.json")  
    
    while True:
        for symbol in stock_array:
            # get data for each stock symbol
            stock_data = get_stock_data_last_week(symbol, start_date_str, end_date_str)
            if stock_data is None:
                print(f"[INFO] Historical data for {symbol} is not available at the moment. Retrying shortly...")
                sleep(2)
                continue
            print(stock_data)
            
            # Send data to Kafka
            send_to_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)  
            
            # Save data to local file
            all_data[symbol] = json.loads(stock_data)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            sleep(2) 
        break

def crawl_stock_realtime_job(symbol, kafka_topic, bootstrap_servers):
    all_data={}
    base_dir = os.path.dirname(os.path.abspath(__file__))  
    file_path = os.path.join(base_dir, "stock_realtime_data.json")
    while True: 
        stock_data = get_stock_data_intraday_realtime(symbol)
        if stock_data is None:
            print(f"[INFO] Realtime data for {symbol} is not available at the moment. Retrying shortly...")
            sleep(60)
            continue
        print(stock_data)
        
        # Send data to Kafka
        send_to_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)
        
        # Save data to local file
        all_data[symbol] = json.loads(stock_data)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        sleep(60)

if __name__ == "__main__":    
    t1 = threading.Thread(target=crawl_vn30_historical_job, args=(KAFKA_TOPIC_HISTORICAL, BOOTSTRAP_SERVERS))
    t2 = threading.Thread(target=crawl_stock_realtime_job, args=('ACB', KAFKA_TOPIC_REALTIME, BOOTSTRAP_SERVERS))

    t1.start()
    t2.start()

    t1.join()
    t2.join()