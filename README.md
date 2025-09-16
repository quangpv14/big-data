# Big Data Platform - Kafka, Spark, HDFS, Elasticsearch, Kibana

## Description

This project sets up a mini big data platform with:
- **Crawler**: Collects stock data and pushes to Kafka.
- **Kafka**: Real-time data ingestion and streaming.
- **HDFS**: Distributed storage for big data.
- **Spark**: Acts as both the batch and speed layer—processing historical data from HDFS and real-time data from Kafka, then writing results back to HDFS.
- **Elasticsearch**: Indexes processed data from Spark for fast search and analytics.
- **Kibana**: Visualizes data stored in Elasticsearch with dashboards and charts.

## Folder Structure

```
big-data/
├── crawler/           # Kafka crawler source code
├── spark/             # Spark streaming source code
├── data/              # HDFS data (git ignored)
├── hue/               # Hue configuration
├── docker-compose.yml # Service definitions
├── .gitignore
└── README.md
```

## How to Run

1. **Clone and build images:**
   ```sh
   git clone ...
   cd big-data
   docker-compose build
   ```

2. **Start all services:**
   ```sh
   docker-compose down -v
   docker-compose up --build -d
   ```

3. **Access services:**
   - Spark Master: "http://localhost:8079/"
   - Kafka: `kafka:9092` (inside Docker network)
   - HDFS: `namenode:8020` (inside Docker network)

## Useful Commands

- **Check HDFS data:**
  ```sh
  docker-compose exec namenode hdfs dfs -ls /user/root/kafka_data
  ```

- **Read Kafka topic data:**
  ```sh
  docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic_name> --from-beginning
  ```

## Notes

- The `data/` folder is git ignored.
- Change topic names or configs in `crawler.py`, `app.py` as needed.
- Default Hue user is `root`.

---

**Contact:**  
If you have issues, please open an issue or contact the development team.