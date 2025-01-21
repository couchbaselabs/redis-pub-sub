# imports
from pub import publish_to_redis
from pub.data_generator import generate_messages
from sub import subscribe_to_keyspace_notifications
import os

if __name__ == "__main__":
    mode = os.getenv("REDIS_MODE", "publish")  # Default to "publish"
    doc_count = int(os.getenv("DOC_COUNT", 100))  # Default to 100

    if mode == "publish":
        print("Generating and publishing 100 messages...")
        messages = generate_messages(doc_count)
        publish_to_redis(messages)
    elif mode == "subscribe":
        print("Starting subscriber...")
        subscribe_to_keyspace_notifications()
    else:
        print(f"Invalid mode: {mode}. Please set REDIS_MODE to 'publish' or 'subscribe'.")
