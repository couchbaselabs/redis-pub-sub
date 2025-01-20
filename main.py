# imports
from pub import publish_to_redis
from sub import subscribe_to_keyspace_notifications

if __name__ == "__main__":
    test_data = "goodbye"
    publish_to_redis(test_data)
    # subscribe_to_keyspace_notifications()
