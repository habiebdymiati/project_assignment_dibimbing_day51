"""
Event Producer for Kafka Transaction Stream
Assignment Day 51 - Streaming for Data Pipeline
Author: Habieb Dymiati Badrul Islam

Produces transaction events to Kafka topic 'transactions' with:
1. Valid transaction events every 1-2 seconds.
2. Minimum 3 Invalid events (negative/too high amount, invalid timestamp, unknown source, duplicates).
3. Minimum 3 Late events (timestamps > 3 minutes in the past for watermark simulation).
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

try:
    from confluent_kafka import Producer
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    HAS_CONFLUENT_KAFKA = False

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("TransactionProducer")

# Constants
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = "transactions"
VALID_SOURCES = ["mobile", "web", "pos"]
SAMPLE_USERS = ["U12345", "U20491", "U31048", "U49201", "U55219", "U67890", "U74123", "U89012"]


def delivery_callback(err, msg):
    """Callback triggered on Kafka broker ack or delivery failure."""
    if err is not None:
        logger.error(f"Delivery failed for message: {err}")
    else:
        key = msg.key().decode("utf-8") if msg.key() else "None"
        logger.debug(
            f"Acked -> topic={msg.topic()} partition=[{msg.partition()}] offset={msg.offset()} key={key}"
        )


def get_current_iso_time(offset_seconds: int = 0) -> str:
    """Returns an ISO-8601 UTC formatted timestamp string with optional time offset."""
    dt = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def create_valid_event(user_id: Optional[str] = None) -> Dict[str, Any]:
    """Generates a valid transaction event."""
    return {
        "user_id": user_id or random.choice(SAMPLE_USERS),
        "amount": round(random.uniform(10_000, 2_500_000), 2),
        "timestamp": get_current_iso_time(),
        "source": random.choice(VALID_SOURCES)
    }


def send_message(producer: Optional[Any], topic: str, event: Dict[str, Any], event_tag: str, note: str = ""):
    """Serializes event and sends to Kafka with formatted logging."""
    payload = json.dumps(event)
    key = str(event.get("user_id") or "UNKNOWN")

    if producer:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload.encode("utf-8"),
            callback=delivery_callback
        )
        producer.poll(0)

    # Clean, informative console log for screenshots and validation
    extra = f" | {note}" if note else ""
    logger.info(f"{event_tag:<32} | Key: {key:<8} | Payload: {payload}{extra}")


def run_producer(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    delay_min: float = 1.0,
    delay_max: float = 2.0,
    mode: str = "all",
    max_events: Optional[int] = None
):
    """
    Main producer routine.
    modes:
      - 'scenarios': Runs the deterministic showcase (valid, invalid, late events) then stops.
      - 'continuous': Runs continuous random streaming.
      - 'all': Runs the showcase first, then continues indefinitely.
    """
    producer = None
    if HAS_CONFLUENT_KAFKA:
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "transaction-event-producer",
            "acks": "all"
        }
        try:
            producer = Producer(conf)
            logger.info(f"Connected to Kafka broker at {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            logger.warning("Proceeding in simulation mode (logging only)...")
    else:
        logger.warning("confluent_kafka not installed. Running in simulation mode (logging only)...")

    logger.info("=" * 80)
    logger.info(f"STARTING KAFKA TRANSACTION PRODUCER -> Topic: '{topic}'")
    logger.info(f"Publish interval: {delay_min}s - {delay_max}s | Mode: {mode}")
    logger.info("=" * 80)

    event_count = 0
    saved_valid_event_for_duplicate = None

    try:
        # =====================================================================
        # PHASE 1: DETERMINISTIC SCENARIO SUITE (Required by Assignment)
        # =====================================================================
        if mode in ("scenarios", "all"):
            logger.info(">>> PHASE 1: Generating Baseline Valid Events (to establish watermark)...")
            for i in range(1, 4):
                evt = create_valid_event(user_id=f"U1000{i}")
                if i == 1:
                    saved_valid_event_for_duplicate = evt.copy()
                send_message(producer, topic, evt, "[VALID EVENT]", f"Baseline valid #{i}")
                event_count += 1
                time.sleep(random.uniform(delay_min, delay_max))

            logger.info(">>> PHASE 2: Generating MINIMAL 3+ INVALID EVENTS...")
            # 1. Invalid: Amount negative
            inv_1 = {
                "user_id": "U12345",
                "amount": -50000,
                "timestamp": get_current_iso_time(),
                "source": "mobile"
            }
            send_message(
                producer, topic, inv_1,
                "[INVALID: NEGATIVE AMOUNT]",
                "amount is negative (-50000), must be >= 1"
            )
            event_count += 1
            time.sleep(random.uniform(delay_min, delay_max))

            # 2. Invalid: Amount exceeds maximum (> 10.000.000)
            inv_2 = {
                "user_id": "U20491",
                "amount": 15000000,
                "timestamp": get_current_iso_time(),
                "source": "web"
            }
            send_message(
                producer, topic, inv_2,
                "[INVALID: AMOUNT TOO LARGE]",
                "amount is 15,000,000 (> 10,000,000)"
            )
            event_count += 1
            time.sleep(random.uniform(delay_min, delay_max))

            # 3. Invalid: Timestamp string is corrupted / invalid
            inv_3 = {
                "user_id": "U31048",
                "amount": 250000,
                "timestamp": "2025-99-99T99:99:99Z",
                "source": "pos"
            }
            send_message(
                producer, topic, inv_3,
                "[INVALID: BAD TIMESTAMP]",
                "timestamp string format is unparseable"
            )
            event_count += 1
            time.sleep(random.uniform(delay_min, delay_max))

            # 4. Invalid: Unknown / unauthorized source channel
            inv_4 = {
                "user_id": "U49201",
                "amount": 175000,
                "timestamp": get_current_iso_time(),
                "source": "desktop_terminal"
            }
            send_message(
                producer, topic, inv_4,
                "[INVALID: UNKNOWN SOURCE]",
                "source 'desktop_terminal' not in ['mobile', 'web', 'pos']"
            )
            event_count += 1
            time.sleep(random.uniform(delay_min, delay_max))

            # 5. Invalid: Duplicate event (exact user_id + timestamp already sent)
            if saved_valid_event_for_duplicate:
                send_message(
                    producer, topic, saved_valid_event_for_duplicate,
                    "[INVALID: DUPLICATE EVENT]",
                    f"Duplicate transaction key ({saved_valid_event_for_duplicate['user_id']}, {saved_valid_event_for_duplicate['timestamp']})"
                )
                event_count += 1
                time.sleep(random.uniform(delay_min, delay_max))

            # 6. Invalid (Extra): Missing mandatory field (user_id is None)
            inv_6 = {
                "user_id": None,
                "amount": 300000,
                "timestamp": get_current_iso_time(),
                "source": "mobile"
            }
            send_message(
                producer, topic, inv_6,
                "[INVALID: MISSING FIELD]",
                "user_id is null / missing"
            )
            event_count += 1
            time.sleep(random.uniform(delay_min, delay_max))

            logger.info(">>> PHASE 3: Generating MINIMAL 3+ LATE EVENTS (Watermark Simulation)...")
            # In Structured Streaming, watermark is set to 3 minutes.
            # Late events must have timestamps older than current time - 3 minutes.
            late_offsets = [
                (-300, "5 minutes late (> 3m watermark)"),
                (-600, "10 minutes late (> 3m watermark)"),
                (-1200, "20 minutes late (> 3m watermark)")
            ]
            for idx, (secs, desc) in enumerate(late_offsets, start=1):
                late_evt = {
                    "user_id": f"U_LATE_{idx}",
                    "amount": 100000 + (idx * 50000),
                    "timestamp": get_current_iso_time(offset_seconds=secs),
                    "source": "mobile"
                }
                send_message(
                    producer, topic, late_evt,
                    f"[LATE EVENT #{idx}: WATERMARK]",
                    f"Timestamp is {desc} -> will route to DLQ"
                )
                event_count += 1
                time.sleep(random.uniform(delay_min, delay_max))

            logger.info(">>> Completed Deterministic Scenario Test Suite.")
            if mode == "scenarios":
                logger.info("Mode 'scenarios' finished. Exiting.")
                return

        # =====================================================================
        # PHASE 2: CONTINUOUS STREAMING WITH RANDOMIZED MIX
        # =====================================================================
        logger.info(">>> Entering Continuous Streaming Mode (Press Ctrl+C to terminate)...")
        while True:
            if max_events and event_count >= max_events:
                logger.info(f"Reached max events limit ({max_events}). Stopping.")
                break

            roll = random.random()
            if roll < 0.70:
                # 70% chance: Normal Valid Transaction
                evt = create_valid_event()
                send_message(producer, topic, evt, "[VALID EVENT]")
            elif roll < 0.85:
                # 15% chance: Randomized Invalid Event
                inv_choice = random.choice(["negative_amt", "huge_amt", "bad_src", "bad_time", "duplicate"])
                if inv_choice == "negative_amt":
                    evt = {
                        "user_id": random.choice(SAMPLE_USERS),
                        "amount": -round(random.uniform(5000, 100000), 2),
                        "timestamp": get_current_iso_time(),
                        "source": random.choice(VALID_SOURCES)
                    }
                    send_message(producer, topic, evt, "[INVALID: NEGATIVE AMOUNT]", "amount < 1")
                elif inv_choice == "huge_amt":
                    evt = {
                        "user_id": random.choice(SAMPLE_USERS),
                        "amount": round(random.uniform(11_000_000, 50_000_000), 2),
                        "timestamp": get_current_iso_time(),
                        "source": random.choice(VALID_SOURCES)
                    }
                    send_message(producer, topic, evt, "[INVALID: AMOUNT TOO LARGE]", "amount > 10,000,000")
                elif inv_choice == "bad_src":
                    evt = {
                        "user_id": random.choice(SAMPLE_USERS),
                        "amount": round(random.uniform(50_000, 500_000), 2),
                        "timestamp": get_current_iso_time(),
                        "source": random.choice(["smart_tv", "iot_gateway", "desktop_cli"])
                    }
                    send_message(producer, topic, evt, "[INVALID: UNKNOWN SOURCE]", f"invalid source {evt['source']}")
                elif inv_choice == "bad_time":
                    evt = {
                        "user_id": random.choice(SAMPLE_USERS),
                        "amount": round(random.uniform(50_000, 500_000), 2),
                        "timestamp": "INVALID-TIMESTAMP-STRING",
                        "source": random.choice(VALID_SOURCES)
                    }
                    send_message(producer, topic, evt, "[INVALID: BAD TIMESTAMP]", "malformed datetime")
                else:
                    # Duplicate
                    dup_user = random.choice(SAMPLE_USERS)
                    dup_ts = get_current_iso_time()
                    evt = {
                        "user_id": dup_user,
                        "amount": 99000,
                        "timestamp": dup_ts,
                        "source": "web"
                    }
                    send_message(producer, topic, evt, "[VALID EVENT]", "Original event")
                    time.sleep(0.5)
                    send_message(producer, topic, evt, "[INVALID: DUPLICATE EVENT]", "Duplicate sent immediately")
            else:
                # 15% chance: Late Event (> 3 minutes old)
                late_min = random.randint(4, 15)
                evt = {
                    "user_id": random.choice(SAMPLE_USERS),
                    "amount": round(random.uniform(20_000, 300_000), 2),
                    "timestamp": get_current_iso_time(offset_seconds=-(late_min * 60)),
                    "source": random.choice(VALID_SOURCES)
                }
                send_message(
                    producer, topic, evt,
                    "[LATE EVENT: WATERMARK]",
                    f"{late_min} minutes late (> 3m watermark)"
                )

            event_count += 1
            time.sleep(random.uniform(delay_min, delay_max))

    except KeyboardInterrupt:
        logger.info("\nCaught KeyboardInterrupt. Gracefully shutting down producer...")
    finally:
        if producer:
            logger.info("Flushing pending events to Kafka broker...")
            producer.flush(timeout=5)
        logger.info(f"Producer stopped. Total events published: {event_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Transaction Event Producer (Day 51)")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})"
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic to publish to (default: {DEFAULT_TOPIC})"
    )
    parser.add_argument(
        "--delay-min",
        type=float,
        default=1.0,
        help="Minimum delay between events in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--delay-max",
        type=float,
        default=2.0,
        help="Maximum delay between events in seconds (default: 2.0)"
    )
    parser.add_argument(
        "--mode",
        choices=["all", "scenarios", "continuous"],
        default="all",
        help="Execution mode: 'all' (scenarios + continuous), 'scenarios' (suite only), 'continuous'"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after publishing max events (default: None, run indefinitely)"
    )

    args = parser.parse_args()
    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        mode=args.mode,
        max_events=args.max_events
    )

