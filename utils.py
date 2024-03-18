import random, string


def generate_device_id() -> str:
    """
    Generate a random device ID
    Example: 1d6dd95a02329496
    """
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(16)
    )
