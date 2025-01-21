import uuid
import random


def generate_messages(num_messages=100):
    """
    Generate a list of random messages for Redis publishing.
    :param num_messages: Number of messages to generate.
    :return: A list of messages (key-value pairs).
    """
    messages = []
    for _ in range(num_messages):
        key = f"test:{uuid.uuid4()}"  # Generate a unique key
        value_type = random.choice(["string", "hash"])  # Randomly choose value type

        if value_type == "string":
            value = f"Message-{random.randint(1, 1000)}"
        else:  # Create a hash-like value
            value = {
                "field1": f"value-{random.randint(1, 1000)}",
                "field2": f"value-{random.randint(1, 1000)}"
            }

        messages.append((key, value))
    return messages
