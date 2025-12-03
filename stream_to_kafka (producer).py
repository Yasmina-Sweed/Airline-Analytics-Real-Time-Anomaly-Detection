import sys
import six
# --- PATCH: Fix for kafka-python compatibility with Python 3.12 ---
sys.modules['kafka.vendor.six.moves'] = six.moves
# ------------------------------------------------------------------
import snowflake.connector
import json
import time
from kafka import KafkaProducer
from datetime import date, datetime
# [cite_start]--- Configuration (Based on your previous inputs) [cite: 1] ---
SNOWFLAKE_CONFIG = {
    'user': 'nancymansour',
    'password': 'Tz5vWhfsfk2ampU',
    'account': 'ys21130.eu-central-2.aws',
    'warehouse': 'COMPUTE_WH',
    'database': 'AIRLINE_PROJECT',
    'schema': 'RAW'
}
TABLE_NAME = "AIRLINE_RAW_DATA"
# ----------------------------------------------------
KAFKA_TOPIC = "airline-flights-2008"
# IMPORTANT: Updated to port 9092 to connect from host to container
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
STREAM_DELAY_SECONDS = 0.5

# Helper to handle date/time objects from Snowflake so they can be serialized to JSON
def json_serializer(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")
def stream_data():
    # 1. Connect to Kafka
    producer = None
    try:
        print(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            # Serialize dictionary data to JSON bytes
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8'),
            # Request timeout in ms (increased slightly for stability)
            request_timeout_ms=60000
        )
        print(f"✅ Connected to Kafka topic: {KAFKA_TOPIC}")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return
    # 2. Connect to Snowflake
    ctx = None
    cs = None
    try:
        print("Connecting to Snowflake...")
        ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cs = ctx.cursor()
        print("✅ Connected to Snowflake.")

        # [cite_start]3. Query Data for 2008 [cite: 1]
        # We use TO_DATE to convert the VARCHAR 'DATE' column and extract the YEAR.
        query = f"SELECT * FROM {TABLE_NAME} WHERE YEAR(TO_DATE(DATE)) = 2008"
        print(f"QUERY: {query}")
        cs.execute(query)

        # Get column names to build dictionaries for JSON output
        columns = [col[0] for col in cs.description]
        print("🚀 Starting to stream data... Press Ctrl+C to stop.")
        count = 0
        for row in cs:
            # Convert tuple row to dictionary using column headers
            row_dict = dict(zip(columns, row))
            # Send the dictionary to Kafka
            producer.send(KAFKA_TOPIC, value=row_dict)
            count += 1
            if count % 100 == 0:
                print(f" -> Sent {count} records...")
                # Force sending buffered data to Kafka periodically
                producer.flush()
            # Simulate real-time streaming delay
            time.sleep(STREAM_DELAY_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping stream due to user interrupt.")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        # 4. Close all connections gracefully
        print("Closing connections...")
        if producer:
            producer.flush()
            producer.close()
        if cs: cs.close()
        if ctx: ctx.close()
        print("Connections closed.")
if __name__ == "__main__":
    stream_data()