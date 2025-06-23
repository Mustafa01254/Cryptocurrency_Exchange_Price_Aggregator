from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, max, min, struct, udf
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

#Load variables from .env file
load_dotenv()

username = os.getenv("CASSANDRA_USERNAME")
password = os.getenv("CASSANDRA_PASSWORD")

#create a spark session
spark = SparkSession.builder \
    .appName("Intergrete_spark_cassandra") \
    .config("spark.cassandra.connection.config.cloud.path", "secure-connect-cryptocurrency.zip") \
    .config("spark.cassandra.auth.username", username) \
    .config("spark.cassandra.auth.password", password) \
    .getOrCreate()


#describe our schema of our message
schema = StructType() \
        .add("platform", StringType()) \
        .add("symbol", StringType()) \
        .add("time", TimestampType()) \
        .add("bid", DoubleType()) \
        .add("ask", DoubleType()) 
        

df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "crypto") \
        .option("startingOffsets", "latest") \
        .load()



df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .selectExpr("data.platform","data.symbol", "data.time", "CAST(data.bid AS DOUBLE) as bid", "CAST(data.ask AS DOUBLE) as ask") 



df_parsed = df_parsed \
    .withWatermark("time", "2 minutes") \
    .withColumn("window", window("time", "1 minute"))

# Define symbol standardization mapping
def standardize_symbol(symbol, platform):
    """
    Convert all symbols to a common format:
    - BTC/USD (Kraken) → BTCUSDT (Binance-style)
    - ETH/USD → ETHUSDT
    - XBT/USD → BTCUSDT (Kraken's Bitcoin symbol)
    """
    symbol = symbol.replace("/", "").upper()
    
    # Handle Kraken's XBT (which means BTC)
    if "XBT" in symbol and platform == "Kraken":
        symbol = symbol.replace("XBT", "BTC")
    
    # Ensure USD pairs end with USDT (Binance style)
    if symbol.endswith("USD"):
        symbol = symbol + "T"
    
    return symbol

# Register UDF
standardize_symbol_udf = udf(standardize_symbol, StringType())

# Apply standardization in your pipeline
df_parsed = df_parsed.withColumn(
    "standardsymbol", 
    standardize_symbol_udf(col("symbol"), col("platform"))
)

best_bids = df_parsed.groupBy("window", "standardsymbol") \
    .agg(
        max(struct(col("bid"), col("platform"))).alias("max_bid_info")
    ) \
    .select(
        "window",
        "standardsymbol",
        col("max_bid_info.bid").alias("best_bid"),
        col("max_bid_info.platform").alias("best_bid_platform")
    )

best_asks = df_parsed.groupBy("window", "standardsymbol") \
    .agg(
        min(struct(col("ask"), col("platform"))).alias("min_ask_info")
    ) \
    .select(
        "window",
        "standardsymbol",
        col("min_ask_info.ask").alias("best_ask"),
        col("min_ask_info.platform").alias("best_ask_platform")
    )
final_result = best_bids.join(best_asks, ["window", "standardsymbol"]) \
        .select(
            "window",
            "standardsymbol",
            "best_bid",
            "best_bid_platform",
            "best_ask",
            "best_ask_platform"
        )

clean_df = final_result.filter(
    col("standardsymbol").isNotNull() & col("window").isNotNull()
)

def handle_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="crypto", keyspace="default_keyspace") \
            .mode("append") \
            .save()
        print(f"successfully wrote batch {batch_id}")

query = clean_df.writeStream \
    .foreachBatch(handle_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()
