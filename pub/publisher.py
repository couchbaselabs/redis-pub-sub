# imports
import redis
import uuid
from datetime import datetime
import json
from config import REDIS_HOST, REDIS_PORT, CHANNEL_NAME


def publish_to_redis(data):
    """
    Publish data to Redis and persist it with a unique key.
    :param data: Data to be published to Redis.
    :return: None
    """
    try:
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

        # Publish message
        r.publish(CHANNEL_NAME, data)
        print(f"Published data to channel '{CHANNEL_NAME}': '{data}'")

        # Generate unique record key
        message_id = str(uuid.uuid4())
        record_key = f"{CHANNEL_NAME}:{message_id}"

        # Persist message
        message_data = {
            "id": message_id,
            "message": data,
            "timestamp": datetime.now().isoformat()
        }
        r.hset(record_key, mapping=message_data)
        print(f"Saved message to Redis hash '{record_key}'")

    except Exception as e:
        print(f"Error: {e}")
