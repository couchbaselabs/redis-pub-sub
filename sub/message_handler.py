# imports
import sys
from time import time
from threading import Timer
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
import redis
from pprint import pprint
from datetime import timedelta
from config import (COUCHBASE_NAME, COUCHBASE_BUCKET, COUCHBASE_USER, COUCHBASE_PASSWORD,
                    REDIS_HOST, REDIS_PORT, CHANNEL_NAME)

# Initialize Redis client
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Initialize Couchbase cluster
try:
    pprint("Initializing Couchbase connection...")
    auth = PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASSWORD)
    cluster = Cluster(
        f"couchbase://{COUCHBASE_NAME}",
        ClusterOptions(auth, timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=5)))
    )
    cluster.wait_until_ready(timedelta(seconds=10))
    pprint("Connected to Couchbase cluster.")

    # Check available buckets
    for bucket in cluster.buckets().get_all_buckets():
        pprint(f"Available bucket: {bucket['name']}")

    # Initialize bucket and collection
    bucket = cluster.bucket(COUCHBASE_BUCKET)
    collection = bucket.default_collection()
except Exception as e:
    pprint(f"Error connecting to Couchbase: {e}")
    sys.exit(1)  # Exit if Couchbase connection fails

# Buffer and batch configurations
message_buffer = []
BATCH_SIZE = 100  # Number of messages to process in a batch
FLUSH_INTERVAL = 1.0  # Interval to flush the buffer (in seconds)


def flush_message_buffer():
    """
    Flush the message buffer by processing the collected messages.
    :return: None
    """
    global message_buffer
    if message_buffer:
        pprint(f"Flushing {len(message_buffer)} messages...")
        process_message_batch(message_buffer)
        message_buffer = []


def start_flush_timer():
    """
    Start a timer to periodically flush the buffer.
    :return: None
    """
    Timer(FLUSH_INTERVAL, flush_message_buffer).start()


def buffer_message(message):
    """
    Add a message to the buffer and flush if the batch size is reached.
    :param message: message to be added to the buffer
    :return: None
    """
    global message_buffer
    message_buffer.append(message)
    if len(message_buffer) >= BATCH_SIZE:
        flush_message_buffer()
    else:
        start_flush_timer()


def process_message_batch(messages):
    """
    Process a batch of messages and write them to Couchbase.
    :param messages: batch of messages
    :return: None
    """
    start_time = time()

    # Ensure `messages` is a list
    if isinstance(messages, dict):
        messages = [messages]

    documents = {}
    pprint(f"Messages received: {repr(messages)}")

    for message in messages:
        try:
            # Validate message structure
            if not isinstance(message, dict) or 'channel' not in message or 'data' not in message:
                pprint(f"Invalid message structure: {repr(message)}")
                continue

            # Extract key and event type
            key_suffix = message['channel'].split(":")[-1]
            key = f"{CHANNEL_NAME}:{key_suffix}"
            event = message['data']

            # Check if the key exists in Redis
            if not redis_client.exists(key):
                pprint(f"Key '{key}' does not exist in Redis.")
                continue

            # Fetch the value from Redis based on the event type
            value = None
            if event == "set":  # For string keys
                value = redis_client.get(key)
            elif event == "hset":  # For hash keys
                value = redis_client.hgetall(key)
                if not isinstance(value, dict):  # Validate the value type
                    pprint(f"Invalid value for hset key '{key}': {repr(value)}")
                    continue

            # Safely log the fetched value
            pprint(f"Fetched value for key '{key}': {repr(value)} (Type: {type(value)})")

            # Prepare the document for Couchbase
            if value:
                documents[key] = {"key": key, "event": event, "value": value}

        except Exception as e:
            pprint(f"Error processing message: {e}")

    # Perform a batch write to Couchbase
    try:
        if documents:
            pprint(f"Performing batch write for {len(documents)} documents...")
            result = collection.upsert_multi(documents)
            pprint(f"Batch write result: {result}")
        else:
            pprint("No valid documents to write.")
    except Exception as cb_exc:
        pprint(f"Failed to write batch to Couchbase: {cb_exc}")

    end_time = time()  # End timing
    elapsed_time_ms = (end_time - start_time) * 1000  # Convert to milliseconds
    pprint(f"PROCESSED BATCH OF {len(documents)} DOCUMENTS IN {elapsed_time_ms:.2f} ms.")
