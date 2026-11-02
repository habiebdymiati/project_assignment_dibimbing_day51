"""
Automated Test Verification for Streaming Data Pipeline (Day 51)
Tests:
- 5 Mandatory validations
- Watermark late event handling (> 3 minutes)
- 1-Minute Tumbling window aggregation
- Console output (timestamp, running_total)
"""

import os
import sys
from datetime import datetime, timezone

# Ensure local loopback binding
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Add current path to sys.path
sys.path.insert(0, os.path.abspath("."))

from pyspark.sql import SparkSession, Row
from streaming.spark_streaming_job import (
    process_batch_factory,
    TRANSACTION_SCHEMA,
    create_spark_session
)


def run_pipeline_test():
    print(">>> Initializing Spark Session for Pipeline Verification...")
    spark = SparkSession.builder \
        .appName("TestPipelineVerification") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Instantiate batch processor
    processor = process_batch_factory(
        bootstrap_servers="localhost:9092",
        valid_topic="transactions_valid",
        dlq_topic="transactions_dlq",
        watermark_seconds=180
    )

    # -------------------------------------------------------------
    # BATCH 1: Valid Baseline Events to establish watermark
    # -------------------------------------------------------------
    print("\n" + "#" * 60)
    print("TESTING BATCH 1: Baseline Valid Transactions")
    print("#" * 60)

    batch_1_data = [
        {"user_id": "U10001", "amount": 150000.0, "timestamp": "2026-11-02T10:00:00Z", "source": "mobile"},
        {"user_id": "U10002", "amount": 250000.0, "timestamp": "2026-11-02T10:00:30Z", "source": "web"},
        {"user_id": "U10003", "amount": 500000.0, "timestamp": "2026-11-02T10:01:00Z", "source": "pos"}
    ]
    batch_1_df = spark.createDataFrame([Row(**d) for d in batch_1_data])
    processor(batch_1_df, 1)

    # -------------------------------------------------------------
    # BATCH 2: Invalid Transactions & Duplicate Detection
    # -------------------------------------------------------------
    print("\n" + "#" * 60)
    print("TESTING BATCH 2: Invalid Events (Amount, Timestamp, Source, Duplicate, Nulls)")
    print("#" * 60)

    batch_2_data = [
        # Valid transaction
        {"user_id": "U10004", "amount": 100000.0, "timestamp": "2026-11-02T10:01:30Z", "source": "mobile"},
        # 1. Invalid: Negative amount
        {"user_id": "U12345", "amount": -50000.0, "timestamp": "2026-11-02T10:01:45Z", "source": "mobile"},
        # 2. Invalid: Amount exceeds 10,000,000
        {"user_id": "U20491", "amount": 15000000.0, "timestamp": "2026-11-02T10:01:50Z", "source": "web"},
        # 3. Invalid: Corrupted timestamp
        {"user_id": "U31048", "amount": 250000.0, "timestamp": "2025-99-99T99:99:99Z", "source": "pos"},
        # 4. Invalid: Unknown source channel
        {"user_id": "U49201", "amount": 175000.0, "timestamp": "2026-11-02T10:01:55Z", "source": "desktop"},
        # 5. Invalid: Duplicate event (exact user_id + timestamp from Batch 1)
        {"user_id": "U10001", "amount": 150000.0, "timestamp": "2026-11-02T10:00:00Z", "source": "mobile"},
        # 6. Invalid: Missing mandatory field
        {"user_id": None, "amount": 300000.0, "timestamp": "2026-11-02T10:01:58Z", "source": "mobile"}
    ]
    batch_2_df = spark.createDataFrame([Row(**d) for d in batch_2_data])
    processor(batch_2_df, 2)

    # -------------------------------------------------------------
    # BATCH 3: Watermark Advance and Late Events (> 3 min)
    # -------------------------------------------------------------
    print("\n" + "#" * 60)
    print("TESTING BATCH 3: Watermark Advance and Late Event Detection")
    print("#" * 60)

    # High watermark will advance to 10:10:00Z (watermark threshold = 10:07:00Z)
    batch_3_data = [
        # Normal event advancing watermark
        {"user_id": "U10005", "amount": 200000.0, "timestamp": "2026-11-02T10:10:00Z", "source": "mobile"},
        # Late event 1: 10:02:00Z (older than 10:07:00Z by 5 minutes) -> DLQ
        {"user_id": "U_LATE_1", "amount": 150000.0, "timestamp": "2026-11-02T10:02:00Z", "source": "mobile"},
        # Late event 2: 09:55:00Z (older than 10:07:00Z by 12 minutes) -> DLQ
        {"user_id": "U_LATE_2", "amount": 200000.0, "timestamp": "2026-11-02T09:55:00Z", "source": "web"},
        # Late event 3: 09:40:00Z (older than 10:07:00Z by 27 minutes) -> DLQ
        {"user_id": "U_LATE_3", "amount": 250000.0, "timestamp": "2026-11-02T09:40:00Z", "source": "pos"}
    ]
    batch_3_df = spark.createDataFrame([Row(**d) for d in batch_3_data])
    processor(batch_3_df, 3)

    print("\n" + "=" * 60)
    print("ALL TEST SCENARIOS EXECUTED SUCCESSFULLY!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    run_pipeline_test()

