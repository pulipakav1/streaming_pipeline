# Real-Time E-Commerce Streaming Pipeline

A production-grade streaming data pipeline that ingests, processes, and visualizes live e-commerce events using Kafka, PySpark Structured Streaming, AWS S3, and Streamlit.

## Architecture

```
[Producer] --> [Kafka Topics x3] --> [PySpark Consumer] --> [AWS S3 Parquet]
                     |
              [Streamlit Dashboard] (reads directly from Kafka)
```


## Live Pipeline 

### Kafka Cluster — 1 Broker, 4 Topics, 53 Partitions
![Kafka UI](k1.png)

### Real-Time Dashboard — 1,624 Orders · $214K Revenue · 94.5% Payment Success
![Dashboard](k2.png)

### AWS S3 — 3 Topic Folders Writing in Real-Time
![S3 Processed](3.png)

### AWS S3 — Date-Partitioned Parquet Output
![S3 Partitioned](k4.png)

### Producer Logs — 10,000 Events at 24 events/sec
![Producer Logs](k5.png)


## Tech Stack

| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka 7.4 |
| Stream processing | PySpark Structured Streaming 3.4 |
| Cloud storage | AWS S3 (Parquet, partitioned by date/hour) |
| Orchestration | Docker Compose |
| Dashboard | Streamlit |
| Language | Python 3.11 |

## Features

- **3 Kafka topics**: `ecommerce.orders`, `ecommerce.clicks`, `ecommerce.payments`
- **80K+ events/hour** throughput (configurable via `EVENTS_PER_SECOND`)
- **Real-time enrichment**: price tiers, engagement scoring, payment success flags
- **S3 output** partitioned by `date/hour` for efficient downstream querying
- **Live dashboard** with auto-refresh every 3 seconds
- **Kafka UI** at `localhost:8080` for topic monitoring
- Fully containerized — runs with one command

## Quick Start

### Prerequisites
- Docker and Docker Compose
- AWS account with S3 bucket

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/ecommerce-streaming
cd ecommerce-streaming

# 2. Configure environment
cp .env.example .env
# Edit .env with your AWS credentials and S3 bucket name

# 3. Start the pipeline
docker-compose up --build
```

### Access
| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |

## Project Structure

```
ecommerce-streaming/
├── producer/
│   ├── producer.py          # Kafka event producer (orders, clicks, payments)
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/
│   ├── consumer.py          # PySpark Structured Streaming consumer
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/
│   ├── dashboard.py         # Streamlit live dashboard
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── .env.example
└── README.md
```

## Event Schema

### Orders (`ecommerce.orders`)
| Field | Type | Description |
|---|---|---|
| event_id | string | Unique event UUID |
| user_id | string | Customer identifier |
| order_id | string | Order identifier |
| product_id | string | Product SKU |
| total_amount | float | Final order value after discount |
| status | string | pending / confirmed / processing |
| city | string | Customer city |
| price_tier | string | budget / mid / premium (enriched) |

### Clicks (`ecommerce.clicks`)
| Field | Type | Description |
|---|---|---|
| session_id | string | Browser session |
| page | string | Page type clicked |
| time_on_page | int | Seconds spent on page |
| engagement | string | bounce / low / medium / high (enriched) |

### Payments (`ecommerce.payments`)
| Field | Type | Description |
|---|---|---|
| payment_id | string | Payment transaction ID |
| payment_method | string | credit_card / paypal / apple_pay etc. |
| status | string | success / failed |
| is_successful | bool | Enriched boolean flag |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `EVENTS_PER_SECOND` | 25 | Producer throughput |
| `S3_BUCKET` | — | Your AWS S3 bucket name |
| `S3_PREFIX` | ecommerce-streaming | S3 key prefix |

## S3 Output Structure

```
s3://your-bucket/ecommerce-streaming/
├── processed/
│   ├── orders/
│   │   └── date=2026-03-13/hour=14/part-0000.parquet
│   ├── clicks/
│   │   └── date=2026-03-13/hour=14/part-0000.parquet
│   └── payments/
│       └── date=2026-03-13/hour=14/part-0000.parquet
└── checkpoints/
    ├── orders/
    ├── clicks/
    └── payments/
```

## Skills Demonstrated

- Apache Kafka topic design and producer/consumer implementation
- PySpark Structured Streaming with multi-topic ingestion
- AWS S3 integration with Hadoop S3A connector
- Stream enrichment and transformation logic
- Docker Compose multi-service orchestration
- Real-time dashboard development with Streamlit
- Production patterns: checkpointing, partitioning, retry logic
