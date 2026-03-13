
import json
import time
import os
import logging
from collections import defaultdict, deque
from datetime import datetime

import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.WARNING)

# config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["ecommerce.orders", "ecommerce.clicks", "ecommerce.payments"]
MAX_HISTORY = 60   # keep last 60 data points for charts

# page setup
st.set_page_config(
    page_title="E-Commerce Live Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.title("🛒 Real-Time E-Commerce Streaming Dashboard")
st.caption(f"Live data from Kafka  ·  Refreshes every 3 seconds  ·  {datetime.now().strftime('%Y-%m-%d')}")

# session state
if "metrics" not in st.session_state:
    st.session_state.metrics = {
        "total_orders":     0,
        "total_clicks":     0,
        "total_payments":   0,
        "total_revenue":    0.0,
        "failed_payments":  0,
        "orders_per_min":   deque(maxlen=MAX_HISTORY),
        "revenue_per_min":  deque(maxlen=MAX_HISTORY),
        "category_counts":  defaultdict(int),
        "city_revenue":     defaultdict(float),
        "device_counts":    defaultdict(int),
        "payment_methods":  defaultdict(int),
        "timestamps":       deque(maxlen=MAX_HISTORY),
    }

m = st.session_state.metrics


# kafka consumer
@st.cache_resource
def get_consumer():
    for attempt in range(5):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="dashboard-consumer",
                consumer_timeout_ms=500,    # non-blocking poll
            )
            return consumer
        except KafkaError:
            time.sleep(2)
    return None


def poll_kafka(consumer, max_messages=500):
    if consumer is None:
        return

    batch_orders  = 0
    batch_revenue = 0.0

    try:
        records = consumer.poll(timeout_ms=500, max_records=max_messages)
        for tp, messages in records.items():
            for msg in messages:
                event = msg.value
                etype = event.get("event_type")

                if etype == "order":
                    m["total_orders"] += 1
                    batch_orders      += 1
                    rev = event.get("total_amount", 0)
                    m["total_revenue"]    += rev
                    batch_revenue         += rev
                    m["category_counts"][event.get("category", "Unknown")] += 1
                    m["city_revenue"][event.get("city", "Unknown")]         += rev
                    m["device_counts"][event.get("device", "Unknown")]      += 1

                elif etype == "click":
                    m["total_clicks"] += 1

                elif etype == "payment":
                    m["total_payments"] += 1
                    m["payment_methods"][event.get("payment_method", "other")] += 1
                    if event.get("status") == "failed":
                        m["failed_payments"] += 1

    except Exception as e:
        st.warning(f"Kafka poll error: {e}")

    # Append time-series point
    m["timestamps"].append(datetime.now().strftime("%H:%M:%S"))
    m["orders_per_min"].append(batch_orders)
    m["revenue_per_min"].append(round(batch_revenue, 2))


#render dashboard
consumer = get_consumer()
poll_kafka(consumer)

# KPI row
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📦 Total Orders",    f"{m['total_orders']:,}")
col2.metric("👆 Total Clicks",    f"{m['total_clicks']:,}")
col3.metric("💳 Total Payments",  f"{m['total_payments']:,}")
col4.metric("💰 Total Revenue",   f"${m['total_revenue']:,.2f}")
payment_success_rate = (
    round((1 - m["failed_payments"] / max(m["total_payments"], 1)) * 100, 1)
)
col5.metric("✅ Payment Success", f"{payment_success_rate}%")

st.divider()

# Charts row 1
left, right = st.columns(2)

with left:
    st.subheader("📈 Orders per Poll Cycle")
    if m["orders_per_min"]:
        import pandas as pd
        df_orders = pd.DataFrame({
            "time":   list(m["timestamps"]),
            "orders": list(m["orders_per_min"])
        }).set_index("time")
        st.line_chart(df_orders, use_container_width=True)
    else:
        st.info("Waiting for data...")

with right:
    st.subheader("💵 Revenue per Poll Cycle")
    if m["revenue_per_min"]:
        import pandas as pd
        df_rev = pd.DataFrame({
            "time":    list(m["timestamps"]),
            "revenue": list(m["revenue_per_min"])
        }).set_index("time")
        st.area_chart(df_rev, use_container_width=True)
    else:
        st.info("Waiting for data...")

st.divider()

# Charts row 2
c1, c2, c3 = st.columns(3)

with c1:
    st.subheader("🗂️ Orders by Category")
    if m["category_counts"]:
        import pandas as pd
        df_cat = pd.DataFrame(
            list(m["category_counts"].items()),
            columns=["Category", "Orders"]
        ).sort_values("Orders", ascending=False)
        st.bar_chart(df_cat.set_index("Category"), use_container_width=True)
    else:
        st.info("Waiting for data...")

with c2:
    st.subheader("📱 Orders by Device")
    if m["device_counts"]:
        import pandas as pd
        df_dev = pd.DataFrame(
            list(m["device_counts"].items()),
            columns=["Device", "Count"]
        )
        st.bar_chart(df_dev.set_index("Device"), use_container_width=True)
    else:
        st.info("Waiting for data...")

with c3:
    st.subheader("💳 Payment Methods")
    if m["payment_methods"]:
        import pandas as pd
        df_pay = pd.DataFrame(
            list(m["payment_methods"].items()),
            columns=["Method", "Count"]
        ).sort_values("Count", ascending=False)
        st.bar_chart(df_pay.set_index("Method"), use_container_width=True)
    else:
        st.info("Waiting for data...")

st.divider()

# Top cities table
st.subheader("🏙️ Top Cities by Revenue")
if m["city_revenue"]:
    import pandas as pd
    df_city = (
        pd.DataFrame(list(m["city_revenue"].items()), columns=["City", "Revenue"])
        .sort_values("Revenue", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    df_city["Revenue"] = df_city["Revenue"].map("${:,.2f}".format)
    st.dataframe(df_city, use_container_width=True, hide_index=True)

# Footer
st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}  ·  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")

# Auto-refresh
time.sleep(3)
st.rerun()
