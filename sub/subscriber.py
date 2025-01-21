# imports
import redis
import signal
import sys
from config import REDIS_HOST, REDIS_PORT
from sub.message_handler import process_message_batch, buffer_message

# Global flag to control the listener loop
is_running = True


def signal_handler(signal, frame):
    """
    Handle the SIGINT signal (Ctrl+C) to stop the subscriber gracefully.
    :param signal:
    :param frame:
    :return:
    """
    global is_running
    print("\nSignal received. Stopping subscriber...")
    is_running = False


def subscribe_to_keyspace_notifications():
    """
    Subscribe to channel and listen for messages.
    :return: None
    """
    global is_running
    try:
        # Connect to Redis
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.psubscribe("__keyspace@0__:*")
        print("Subscribed to all keyspace notifications in database 0. Press Ctrl+C to stop.")

        for message in pubsub.listen():
            if message['type'] == 'pmessage':
                buffer_message(message)  # Buffer the message
    except Exception as e:
        print(f"Error: {e}")
    finally:
        pubsub.close()
        print("Subscriber connection closed.")


# Register the signal handler for Ctrl+C
signal.signal(signal.SIGINT, signal_handler)
