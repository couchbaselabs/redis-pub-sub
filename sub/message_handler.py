# imports
import sys
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
import redis
from datetime import timedelta
from config import (COUCHBASE_NAME, COUCHBASE_BUCKET, COUCHBASE_USER, COUCHBASE_PASSWORD,
                    REDIS_HOST, REDIS_PORT, CHANNEL_NAME)

# Initialize Redis client
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Initialize Couchbase cluster
try:
    print("Initializing Couchbase connection...")
    auth = PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASSWORD)
    cluster = Cluster(
        f"couchbase://{COUCHBASE_NAME}",
        ClusterOptions(auth, timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=5)))
    )
    cluster.wait_until_ready(timedelta(seconds=10))
    print("Connected to Couchbase cluster.")

    # Check available buckets
    for bucket in cluster.buckets().get_all_buckets():
        print(f"Available bucket: {bucket['name']}")

    # Initialize bucket and collection
    bucket = cluster.bucket(COUCHBASE_BUCKET)
    collection = bucket.default_collection()
except Exception as e:
    print(f"Error connecting to Couchbase: {e}")
    sys.exit(1)  # Exit if Couchbase connection fails


def process_message(message):
    """
    Process incoming keyspace notifications and write data to Couchbase.
    """
    try:
        # Log the raw message for debugging
        print(f"Raw message: {repr(message)}")  # Use repr() for safe printing

        # Extract key and event type
        key_suffix = message['channel'].split(":")[-1]  # Extract the Redis key suffix
        key = f"{CHANNEL_NAME}:{key_suffix}"  # Build the full Redis key
        event = message['data']
        print(f"Processing -> Key: {key}, Event: {event}")

        # Check if the key exists in Redis
        if not redis_client.exists(key):
            print(f"Key '{key}' does not exist in Redis.")
            return

        # Fetch the value from Redis based on the event type
        value = None
        if event == "set":
            value = redis_client.get(key)
        elif event == "hset":
            value = redis_client.hgetall(key)

        # Safely log the fetched value
        print(f"Fetched value for key '{key}': {repr(value)}")  # Use repr() for clarity

        # Write the data into Couchbase
        if value:
            doc_id = key  # Use the Redis key as the Couchbase document ID
            document = {"key": key, "event": event, "value": value}
            try:
                collection.upsert(doc_id, document)
                print(f"Successfully wrote document to Couchbase with ID: {doc_id}")
            except Exception as cb_exc:
                print(f"Failed to write to Couchbase: {cb_exc}")

    except Exception as e:
        print(f"Error processing message: {e}")
