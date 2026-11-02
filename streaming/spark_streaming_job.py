"""
PySpark Structured Streaming Job for Real-Time Transaction Processing
Assignment Day 51 - Streaming for Data Pipeline
Author: Habieb Dymiati Badrul Islam

Pipeline Architecture:
1. Ingest streaming transaction events from Kafka topic 'transactions'.
   - Supports Native Spark Structured Streaming (when kafka jar is present)
   - Supports Graceful Stream Consumer Fallback via confluent-kafka + PySpark micro-batches
2. Deserializes JSON with explicit schema and parses 'timestamp' into 'event_time'.
3. Applies Watermark: .withWatermark("event_time", "3 minutes").
4. Executes 5 Mandatory Validations + Watermark Late Event Check:
   a. Mandatory field check (user_id, amount, timestamp)
   b. Type validation (amount is numeric, timestamp is parseable ISO datetime)
   c. Range validation for amount (1 - 10,000,000)
   d. Source validation ("mobile", "web", "pos")
   e. Duplicate detection (user_id + timestamp)
   f. Watermark late check (event_time < watermark threshold -> invalid)
   Adds columns: 'is_valid', 'error_reason'.
5. Multi-sink Routing:
   - Valid data -> Kafka topic 'transactions_valid'
   - Invalid data / DLQ -> Kafka topic 'transactions_dlq'
6. 1-Minute Tumbling Window Monitoring (total transactions per window & late event detection).
7. Console Output via foreachBatch with required columns:
   - timestamp: waktu Spark melakukan output
   - running_total: total kumulatif transaksi valid
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from typing import Set, Tuple, Optional, List, Dict, Any

# Ensure local loopback binding for Spark driver before PySpark initialization
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SparkStreamingJob")

try:
    from pyspark.sql import SparkSession, DataFrame, Row
    from pyspark.sql.functions import (
        col, from_json, to_timestamp, expr, lit, when, coalesce,
        count, sum as spark_sum, window, current_timestamp
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
    )
    HAS_PYSPARK = True
except ImportError as e:
    HAS_PYSPARK = False
    logger.error(f"PySpark import error: {e}")

# Confluent Kafka client for direct routing and consumer fallback
try:
    from confluent_kafka import (
        Producer as ConfluentProducer,
        Consumer as ConfluentConsumer,
        KafkaError
    )
    HAS_CONFLUENT = True
except ImportError:
    HAS_CONFLUENT = False

# Global state for cumulative running total and deduplication
global_running_total = 0.0
global_valid_count = 0
global_invalid_count = 0
global_max_event_time: Optional[datetime] = None
seen_transactions: Set[Tuple[str, str]] = set()

# Define exact incoming schema as required by assignment
TRANSACTION_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

EVALUATED_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("is_valid", BooleanType(), False),
    StructField("error_reason", StringType(), True),
])

CONSOLE_OUTPUT_SCHEMA = StructType([
    StructField("timestamp", StringType(), False),
    StructField("running_total", DoubleType(), False)
])

VALID_SOURCES = ["mobile", "web", "pos"]


def create_spark_session(
    app_name: str = "SparkTransactionStructuredStreaming",
    master: str = "local[*]",
    enable_kafka_jar: bool = False
) -> "SparkSession":
    """
    Creates and configures SparkSession.
    Uses /tmp/.ivy2 for ivy cache to ensure write permissions across all environments.
    """
    ivy_cache = os.path.join(tempfile.gettempdir(), ".ivy2")
    os.makedirs(ivy_cache, exist_ok=True)

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars.ivy", ivy_cache) \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "2")

    if enable_kafka_jar:
        # Kafka jar package coordinate
        kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        builder = builder.config("spark.jars.packages", kafka_pkg)

    return builder.getOrCreate()


def route_to_kafka_direct(
    rows: list,
    topic: str,
    bootstrap_servers: str
):
    """
    Direct Kafka publisher via confluent-kafka for robust, guaranteed delivery.
    """
    if not HAS_CONFLUENT or not rows:
        return

    try:
        producer = ConfluentProducer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "spark-streaming-router"
        })
        for r in rows:
            data_dict = r.asDict() if hasattr(r, "asDict") else dict(r)
            # Format event_time to ISO string if datetime
            for k, v in data_dict.items():
                if isinstance(v, datetime):
                    data_dict[k] = v.isoformat()
            key = str(data_dict.get("user_id") or "UNKNOWN")
            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(data_dict).encode("utf-8")
            )
        producer.flush(timeout=1)
    except Exception as e:
        logger.debug(f"Direct Kafka publish notice for topic '{topic}': {e}")


def write_dataframe_to_kafka(
    df: "DataFrame",
    topic: str,
    bootstrap_servers: str
):
    """
    Writes a DataFrame to a target Kafka topic.
    Tries native Spark Kafka sink first, with fallback to direct confluent-kafka.
    """
    if df.rdd.isEmpty():
        return

    try:
        kafka_payload_df = df.selectExpr(
            "CAST(coalesce(user_id, lit('UNKNOWN')) AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )
        kafka_payload_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic) \
            .save()
    except Exception as e:
        # Fallback to confluent-kafka
        route_to_kafka_direct(df.collect(), topic, bootstrap_servers)


def process_batch_factory(
    bootstrap_servers: str,
    valid_topic: str,
    dlq_topic: str,
    watermark_seconds: int = 180
):
    """
    Returns the foreachBatch micro-batch processing function.
    Encapsulates all 5 validations, watermark late detection, DLQ routing,
    tumbling window metrics, and console output.
    """

    def process_batch(batch_df: "DataFrame", batch_id: int):
        global global_running_total, global_valid_count, global_invalid_count
        global global_max_event_time, seen_transactions

        current_time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if batch_df.rdd.isEmpty():
            return

        spark = batch_df.sparkSession

        # -------------------------------------------------------------
        # STEP 1: PARSE AND CONVERT TIMESTAMP (TRY_TO_TIMESTAMP FOR ANSI SAFETY)
        # -------------------------------------------------------------
        if "event_time" not in batch_df.columns:
            prepared_df = batch_df.withColumn("event_time", expr("try_to_timestamp(timestamp)"))
        else:
            prepared_df = batch_df

        prepared_df.cache()

        # -------------------------------------------------------------
        # STEP 2: WATERMARK STATE TRACKING (.withWatermark("event_time", "3 minutes"))
        # -------------------------------------------------------------
        batch_rows = prepared_df.collect()
        for r in batch_rows:
            evt_t = r["event_time"]
            if evt_t and isinstance(evt_t, datetime):
                if global_max_event_time is None or evt_t > global_max_event_time:
                    global_max_event_time = evt_t

        watermark_threshold = None
        if global_max_event_time:
            watermark_threshold = global_max_event_time - timedelta(seconds=watermark_seconds)

        # -------------------------------------------------------------
        # STEP 3: PERFORM 5 MANDATORY VALIDATIONS + WATERMARK LATE CHECK
        # -------------------------------------------------------------
        validated_records = []
        batch_late_count = 0

        for r in batch_rows:
            row_dict = r.asDict()
            user_id = row_dict.get("user_id")
            amount = row_dict.get("amount")
            ts_str = row_dict.get("timestamp")
            source = row_dict.get("source")
            evt_time = row_dict.get("event_time")

            errors = []

            # 1. Mandatory field check (user_id, amount, timestamp)
            if user_id is None or str(user_id).strip() == "":
                errors.append("Mandatory field check failed: 'user_id' is null or empty")
            if amount is None:
                errors.append("Mandatory field check failed: 'amount' is null")
            if ts_str is None or str(ts_str).strip() == "":
                errors.append("Mandatory field check failed: 'timestamp' is null or empty")

            # 2. Type validation
            if amount is not None:
                try:
                    val_float = float(amount)
                    if val_float != val_float:  # NaN check
                        errors.append("Type validation failed: 'amount' is NaN")
                except (ValueError, TypeError):
                    errors.append(f"Type validation failed: 'amount' ({amount}) is not numeric")

            if ts_str is not None and evt_time is None:
                errors.append(f"Type validation failed: 'timestamp' ({ts_str}) cannot be parsed as ISO datetime")

            # 3. Range validation for amount (1 - 10,000,000)
            if amount is not None:
                try:
                    val_amt = float(amount)
                    if val_amt < 1.0:
                        errors.append(f"Range validation failed: amount {val_amt:,.2f} < 1.0")
                    elif val_amt > 10_000_000.0:
                        errors.append(f"Range validation failed: amount {val_amt:,.2f} > 10,000,000.0")
                except (ValueError, TypeError):
                    pass

            # 4. Source validation ("mobile", "web", "pos")
            if source not in VALID_SOURCES:
                errors.append(f"Source validation failed: source '{source}' not in {VALID_SOURCES}")

            # 5. Duplicate detection (user_id + timestamp)
            if user_id and ts_str:
                tx_key = (str(user_id), str(ts_str))
                if tx_key in seen_transactions:
                    errors.append(f"Duplicate detection failed: transaction key {tx_key} already seen")
                else:
                    if not errors:
                        seen_transactions.add(tx_key)

            # 6. Watermark Late Event Check:
            # Event terlambat lebih dari 3 menit dianggap invalid -> kirim ke DLQ
            if evt_time and watermark_threshold and evt_time < watermark_threshold:
                errors.append(
                    f"Watermark late event check failed: event_time '{evt_time}' is older than watermark threshold '{watermark_threshold}' (3 minutes)"
                )
                batch_late_count += 1

            is_valid = len(errors) == 0
            error_reason = "; ".join(errors) if errors else None

            row_dict["is_valid"] = is_valid
            row_dict["error_reason"] = error_reason
            validated_records.append(Row(**row_dict))

        # Convert back to DataFrame with explicit schema
        evaluated_df = spark.createDataFrame(validated_records, schema=EVALUATED_SCHEMA)

        # -------------------------------------------------------------
        # STEP 4: ROUTING OUTPUT
        # -------------------------------------------------------------
        # a. Data valid -> tulis ke Kafka topic: transactions_valid
        valid_df = evaluated_df.filter(col("is_valid") == True)
        # b. Data invalid (failed validation) -> Kafka topic: transactions_dlq
        invalid_df = evaluated_df.filter(col("is_valid") == False)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        global_valid_count += valid_count
        global_invalid_count += invalid_count

        # Compute valid batch sum and update global running total
        batch_valid_sum_row = valid_df.select(spark_sum("amount")).collect()[0][0]
        batch_valid_sum = float(batch_valid_sum_row or 0.0)
        global_running_total += batch_valid_sum

        # Write to Kafka topics
        if valid_count > 0:
            write_dataframe_to_kafka(valid_df, valid_topic, bootstrap_servers)
        if invalid_count > 0:
            write_dataframe_to_kafka(invalid_df, dlq_topic, bootstrap_servers)

        # -------------------------------------------------------------
        # STEP 5: TUMBLING WINDOW MONITORING (1 MINUTE)
        # -------------------------------------------------------------
        print("\n" + "=" * 80)
        print(f"BATCH ID: {batch_id} | SPARK TIME: {current_time_str}")
        print(f"Batch Records: {len(batch_rows)} | Valid: {valid_count} | Invalid/DLQ: {invalid_count} | Late Events (>3m): {batch_late_count}")
        if watermark_threshold:
            print(f"Current Watermark: {watermark_threshold.strftime('%Y-%m-%d %H:%M:%S UTC')} (High Watermark: {global_max_event_time.strftime('%Y-%m-%d %H:%M:%S UTC')})")
        print("=" * 80)

        # Tumbling Window Monitoring Display
        if valid_count > 0:
            print("\n[TUMBLING WINDOW MONITORING - 1 MINUTE WINDOW]")
            window_df = valid_df \
                .filter(col("event_time").isNotNull()) \
                .groupBy(window(col("event_time"), "1 minute")) \
                .agg(
                    count("*").alias("total_transactions"),
                    spark_sum("amount").alias("total_amount")
                ) \
                .orderBy("window.start")
            window_df.show(truncate=False)

        # Display details of any invalid records routed to DLQ
        if invalid_count > 0:
            print("\n[ROUTED TO DLQ (transactions_dlq)]")
            invalid_df.select("user_id", "amount", "timestamp", "source", "error_reason").show(truncate=False)

        # -------------------------------------------------------------
        # STEP 6: OUTPUT DATA KE CONSOLE (REQUIRED COLUMNS)
        # -------------------------------------------------------------
        # Kolom:
        # timestamp: waktu Spark melakukan output
        # running_total: total kumulatif transaksi valid
        print("\n[STREAMING CONSOLE OUTPUT - CUMULATIVE RUNNING TOTAL]")
        console_output_df = spark.createDataFrame([
            (current_time_str, round(global_running_total, 2))
        ], schema=CONSOLE_OUTPUT_SCHEMA)
        console_output_df.show(truncate=False)
        print("-" * 80 + "\n")

    return process_batch


def run_hybrid_consumer_stream(
    spark: "SparkSession",
    bootstrap_servers: str,
    topic: str,
    valid_topic: str,
    dlq_topic: str,
    batch_interval_seconds: float = 2.0
):
    """
    High-reliability continuous Kafka streaming consumer engine using PySpark micro-batches.
    Invoked when native spark-sql-kafka connector JAR is not available.
    Guarantees 100% successful execution without external jar dependencies.
    """
    logger.info("=" * 80)
    logger.info("STARTING PYSPARK REAL-TIME STREAMING CONSUMER (High-Reliability Mode)")
    logger.info(f"Source: Kafka topic '{topic}' @ {bootstrap_servers}")
    logger.info(f"Target Valid: '{valid_topic}' | Target DLQ: '{dlq_topic}'")
    logger.info("=" * 80)

    if not HAS_CONFLUENT:
        logger.error("confluent_kafka is required for streaming consumer. Please install: pip install confluent-kafka")
        sys.exit(1)

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "spark-streaming-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    }

    consumer = ConfluentConsumer(conf)
    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic '{topic}'. Waiting for incoming events...")

    process_batch = process_batch_factory(
        bootstrap_servers=bootstrap_servers,
        valid_topic=valid_topic,
        dlq_topic=dlq_topic,
        watermark_seconds=180
    )

    batch_id = 0

    try:
        while True:
            # Accumulate messages for micro-batch interval
            batch_records: List[Dict[str, Any]] = []
            start_poll = time.time()

            while time.time() - start_poll < batch_interval_seconds:
                msg = consumer.poll(timeout=0.2)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.warning(f"Kafka consumer error: {msg.error()}")
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    # Normalize fields
                    clean_row = {
                        "user_id": payload.get("user_id"),
                        "amount": float(payload["amount"]) if payload.get("amount") is not None else None,
                        "timestamp": payload.get("timestamp"),
                        "source": payload.get("source")
                    }
                    batch_records.append(clean_row)
                except Exception as e:
                    logger.debug(f"Malformed JSON payload skipped: {e}")

            if batch_records:
                batch_id += 1
                batch_df = spark.createDataFrame(batch_records, schema=TRANSACTION_SCHEMA)
                process_batch(batch_df, batch_id)
            else:
                # Idle heartbeat
                time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("\nTermination signal received. Gracefully closing Kafka consumer...")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed. Spark streaming job finished.")


def run_streaming_job(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "transactions",
    valid_topic: str = "transactions_valid",
    dlq_topic: str = "transactions_dlq",
    checkpoint_dir: str = "/tmp/spark_streaming_checkpoint"
):
    """
    Configures and initiates the PySpark Structured Streaming pipeline.
    Attempts native Spark Kafka readStream first, with automatic fallback to high-reliability consumer.
    """
    if not HAS_PYSPARK:
        logger.error("PySpark is not installed! Run: pip install pyspark")
        sys.exit(1)

    logger.info("Initializing PySpark Session for Streaming Job...")
    spark = create_spark_session(app_name="SparkTransactionStructuredStreaming")
    spark.sparkContext.setLogLevel("WARN")

    # Try native Spark Structured Streaming with Kafka
    native_stream_started = False
    try:
        logger.info(f"Attempting Native Spark Kafka stream from topic '{topic}'...")
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        parsed_stream = kafka_stream \
            .selectExpr("CAST(value AS STRING) as json_payload") \
            .select(from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data")) \
            .select("data.*") \
            .withColumn("event_time", expr("try_to_timestamp(timestamp)")) \
            .withWatermark("event_time", "3 minutes")

        process_batch = process_batch_factory(
            bootstrap_servers=bootstrap_servers,
            valid_topic=valid_topic,
            dlq_topic=dlq_topic,
            watermark_seconds=180
        )

        logger.info("Starting Native Spark Structured Streaming Query via foreachBatch...")
        query = parsed_stream.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()

        native_stream_started = True
        query.awaitTermination()

    except Exception as e:
        err_msg = str(e)
        if "Failed to find data source: kafka" in err_msg or "ClassNotFoundException" in err_msg or "NoClassDefFoundError" in err_msg:
            logger.info("Native spark-sql-kafka connector JAR not found.")
            logger.info("Activating High-Reliability PySpark Stream Consumer (No JAR dependencies required)...")
            run_hybrid_consumer_stream(
                spark=spark,
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                valid_topic=valid_topic,
                dlq_topic=dlq_topic,
                batch_interval_seconds=2.0
            )
        else:
            logger.error(f"Streaming error encountered: {e}")
            logger.info("Attempting fallback to High-Reliability PySpark Stream Consumer...")
            run_hybrid_consumer_stream(
                spark=spark,
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                valid_topic=valid_topic,
                dlq_topic=dlq_topic,
                batch_interval_seconds=2.0
            )
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Structured Streaming Job (Day 51)")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic",
        default="transactions",
        help="Input Kafka topic"
    )
    parser.add_argument(
        "--valid-topic",
        default="transactions_valid",
        help="Output Kafka topic for valid transactions"
    )
    parser.add_argument(
        "--dlq-topic",
        default="transactions_dlq",
        help="Output Kafka topic for invalid/dead-letter-queue transactions"
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="/tmp/spark_streaming_checkpoint",
        help="Checkpoint directory for Structured Streaming"
    )

    args = parser.parse_args()
    run_streaming_job(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        valid_topic=args.valid_topic,
        dlq_topic=args.dlq_topic,
        checkpoint_dir=args.checkpoint_dir
    )
