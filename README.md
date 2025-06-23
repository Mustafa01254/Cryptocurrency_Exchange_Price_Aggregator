# Cryptocurrency_Exchange_Price_Aggregator
Fetches cryptocurrency data from multiple platforms in real-time, aggregates prices, and exposes the best bid/ask prices. Built with Apache Spark, Kafka, and Cassandra.

# Features
**Real-Time Streaming**: Ingests live data using Kafka topic.
**Persistent storage**: Stores the latest prices in the Cassandra database (Astra).
**Scalability**: Built using distributed technologies like Spark and Cassandra.

#Technologies used
**Apache Kafka**: Ingest live market data feeds.
**Apache Spark**: Stream processing and transformation.
**Apache Cassandra**: Data storage.
**Astra DB**: Cloud-hosted cassandra DB.
