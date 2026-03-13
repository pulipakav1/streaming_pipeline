import json
import time
import uuid
import random
import logging
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND       = int(os.getenv("EVENTS_PER_SECOND", "25"))

TOPICS = {
    "orders":   "ecommerce.orders",
    "clicks":   "ecommerce.clicks",
    "payments": "ecommerce.payments",
}

# data pools sample
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones",  "category": "Electronics", "price": 89.99},
    {"id": "P002", "name": "Running Shoes",         "category": "Sports",      "price": 129.99},
    {"id": "P003", "name": "Coffee Maker",          "category": "Kitchen",     "price": 59.99},
    {"id": "P004", "name": "Yoga Mat",              "category": "Sports",      "price": 34.99},
    {"id": "P005", "name": "Bluetooth Speaker",     "category": "Electronics", "price": 49.99},
    {"id": "P006", "name": "Desk Lamp",             "category": "Home",        "price": 24.99},
    {"id": "P007", "name": "Water Bottle",          "category": "Sports",      "price": 19.99},
    {"id": "P008", "name": "Laptop Stand",          "category": "Electronics", "price": 44.99},
    {"id": "P009", "name": "Scented Candle",        "category": "Home",        "price": 14.99},
    {"id": "P010", "name": "Protein Powder",        "category": "Health",      "price": 54.99},
]

CITIES    = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
             "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
DEVICES   = ["mobile", "desktop", "tablet"]
OS_LIST   = ["iOS", "Android", "Windows", "macOS"]
BROWSERS  = ["Chrome", "Safari", "Firefox", "Edge"]
PAY_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
STATUSES  = ["pending", "confirmed", "processing"]


# event generators

def generate_user_id() -> str:
    return f"U{random.randint(1000, 9999)}"

def generate_session_id() -> str:
    return str(uuid.uuid4())[:8]

def generate_order_event() -> dict:
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    discount = round(random.uniform(0, 0.3), 2)
    subtotal = round(product["price"] * quantity, 2)
    total    = round(subtotal * (1 - discount), 2)

    return {
        "event_type":   "order",
        "event_id":     str(uuid.uuid4()),
        "timestamp":    datetime.utcnow().isoformat(),
        "user_id":      generate_user_id(),
        "session_id":   generate_session_id(),
        "order_id":     f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "product_id":   product["id"],
        "product_name": product["name"],
        "category":     product["category"],
        "quantity":     quantity,
        "unit_price":   product["price"],
        "discount":     discount,
        "subtotal":     subtotal,
        "total_amount": total,
        "currency":     "USD",
        "status":       random.choice(STATUSES),
        "city":         random.choice(CITIES),
        "device":       random.choice(DEVICES),
    }

def generate_click_event() -> dict:
    product = random.choice(PRODUCTS)
    return {
        "event_type":   "click",
        "event_id":     str(uuid.uuid4()),
        "timestamp":    datetime.utcnow().isoformat(),
        "user_id":      generate_user_id(),
        "session_id":   generate_session_id(),
        "product_id":   product["id"],
        "product_name": product["name"],
        "category":     product["category"],
        "page":         random.choice(["home", "search", "category", "product", "cart"]),
        "device":       random.choice(DEVICES),
        "os":           random.choice(OS_LIST),
        "browser":      random.choice(BROWSERS),
        "city":         random.choice(CITIES),
        "time_on_page": random.randint(5, 300),
    }

def generate_payment_event() -> dict:
    product = random.choice(PRODUCTS)
    amount  = round(product["price"] * random.randint(1, 3), 2)
    success = random.random() > 0.05   

    return {
        "event_type":      "payment",
        "event_id":        str(uuid.uuid4()),
        "timestamp":       datetime.utcnow().isoformat(),
        "user_id":         generate_user_id(),
        "order_id":        f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "payment_id":      f"PAY-{uuid.uuid4().hex[:8].upper()}",
        "amount":          amount,
        "currency":        "USD",
        "payment_method":  random.choice(PAY_METHODS),
        "status":          "success" if success else "failed",
        "failure_reason":  None if success else random.choice(
                               ["insufficient_funds", "card_expired", "declined"]),
        "city":            random.choice(CITIES),
        "device":          random.choice(DEVICES),
    }


# producer

def create_producer() -> KafkaProducer:
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",                  
                retries=3,
                linger_ms=10,               
                batch_size=16384,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            logger.warning(f"Attempt {attempt+1}/10 failed: {e}. Retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def on_send_error(exc):
    logger.error(f"Failed to send message: {exc}")


def run():
    producer   = create_producer()
    sleep_time = 1.0 / EVENTS_PER_SECOND

    event_weights = [
        (generate_click_event,   TOPICS["clicks"],   0.50),
        (generate_order_event,   TOPICS["orders"],   0.30),
        (generate_payment_event, TOPICS["payments"], 0.20),
    ]

    generators  = [g for g, _, w in event_weights for _ in range(int(w * 100))]
    topic_map   = {}
    for g, t, w in event_weights:
        topic_map[g] = t

    sent_count  = 0
    start_time  = time.time()
    log_interval = 1000

    logger.info(f"Starting producer — {EVENTS_PER_SECOND} events/sec across 3 topics")

    try:
        while True:
            gen      = random.choice(generators)
            event    = gen()
            topic    = topic_map[gen]
            key      = event.get("user_id", "unknown")

            producer.send(topic, key=key, value=event).add_errback(on_send_error)

            sent_count += 1
            if sent_count % log_interval == 0:
                elapsed = time.time() - start_time
                rate    = sent_count / elapsed
                logger.info(f"Sent {sent_count:,} events | Rate: {rate:.0f} events/sec")

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Total events sent: {sent_count:,}")


if __name__ == "__main__":
    run()
