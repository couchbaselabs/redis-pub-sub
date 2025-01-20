def handle_message(message):
    """
    Handle incoming messages from the Redis channel.
    :param message: The message received from the channel.
    """
    print(f"Keyspace Notification: {message['channel']} -> {message['data']}")
    print(message)

