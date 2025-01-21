import redis
from config import REDIS_HOST, REDIS_PORT, CHANNEL_NAME
from pub.data_generator import generate_messages


def publish_to_redis(messages):
    """
    Publish multiple messages to Redis.
    :param messages: List of tuples containing keys and values to publish.
    """
    try:
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        for key, value in messages:
            if isinstance(value, dict):  # If value is a hash
                r.hset(key, mapping=value)
                print(f"Published hash to Redis: {key} -> {value}")
            else:  # If value is a string
                r.set(key, value)
                print(f"Published string to Redis: {key} -> {value}")

            # Optionally publish to a channel
            r.publish(CHANNEL_NAME, key)
            print(f"Published key '{key}' to channel '{CHANNEL_NAME}'")

    except Exception as e:
        print(f"Error publishing to Redis: {e}")
