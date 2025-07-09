# Cryptocurrency_Exchange_Price_Aggregator
Fetches cryptocurrency data from multiple platforms in real-time, aggregates prices, and exposes the best bid/ask prices. Built with Apache Spark, Kafka, and Cassandra.

# Features
* **Real-Time Streaming**: Ingests live data using a Kafka topic.
* **Persistent storage**: Stores the latest prices in the Cassandra database (Astra).
* **Scalability**: Built using distributed technologies like Spark and Cassandra.

# Technologies used
* **Apache Kafka**: Ingest live market data feeds.
* **Apache Spark**: Stream processing and transformation.
* **Apache Cassandra**: Data storage.
* **Astra DB**: Cloud-hosted cassandra DB.

# 🚀  Quick Start
# Prerequisites
  * Python
  * Java, JDK 8 or 11
  * Scala
  * Spark
  * Kafka
  * Cassandra or Astra DB account

# Installations
1. Clone the repository to your local machine
```
git clone https://github.com/your_username/Cryptocurrency_Exchange_Price_Aggregator.git && cd Cryptocurrency_Exchange_Price_Aggregator
```
2. Create an environment
```
python3 -m venv venv
```
3. Activate the environment
```
. venv/bin/activate
```
4. Install Python dependencies
```
pip install -r requirements.txt
```

# Environment Configuration
Make sure you have a .env file in the project root with the following:
```
CASSANDRA_USERNAME=your_astra_username
CASSANDRA_PASSWORD=your_astra_password
```

# Kafka Setup
You need to start Kafka, create the required topic, and optionally run a producer and consumer to test.
1. Start Kafka and Zookeeper (Local)
```
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# In a new terminal: Start Kafka
bin/kafka-server-start.sh config/server.properties
```

2. Create a topic
Create a kafka topic that spark will consume
```bin/kafka-topics.sh --create --topic crypto --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

 # Confirm if created
bin/kafka-topics.sh --list --bootstrap-server 127.0.0.1:9092
```

3. Consume Messages
Start the consumer:
```
bin/kafka-console-consumer.sh --topic crypto-prices --bootstrap-server 127.0.0.1:9092 --from-beginning
```

# Run the websocket client
```
# binance ws
python3 binance_websocket.py

# Kraken ws
python3 kraken_ws.py
```

The WebSocket client listens for real-time updates and forwards them to Kafka.

After running the webclient, messages should start streaming on the Kafka topic:

![image](https://github.com/user-attachments/assets/8a7f3b9c-6d7b-45d3-9ae2-86cf203bdc83)


# Astra DB table
Create a table using Datastax CQL Console:
```
CREATE TABLE IF NOT EXISTS crypto.prices (
    window text,
    standardsymbol text,  -- use timestamp if using ISO 8601 in your schema
    best_bid double,
    best_bid_platform text,
    best_ask double,
    best_ask_platform text,
    PRIMARY KEY (window, standardsymbol)
);
```

# Run Spark Streaming Job
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
--conf spark.files=/path/to/secure-connect-cryptocurrency.zip \
--conf spark.cassandra.connection.config.cloud.path=secure-connect-cryptocurrency.zip load.py
```

![image](https://github.com/user-attachments/assets/d499b51f-2e4a-4cbf-9603-718bcf395047)







