from kafka import KafkaProducer

import os
from time import sleep
import json 

from random import choices, randint
from string import ascii_letters, digits

from transactions import create_random_transaction

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
TRANSACTIONS_PER_SECOND = float(os.environ.get("TRANSACTIONS_PER_SECOND"))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

account_chars: str = digits + ascii_letters

def _random_account_id() -> str:
    """Return a random account number made of 12 characters."""
    return "".join(choices(account_chars, k=12))

def _random_amount() -> float:
    """Return a random amount between 1.00 and 1000.00."""
    return randint(100, 1000000) / 100

def create_random_transaction() -> dict:
    """Create a fake, randomised transaction."""
    return {
    "source": _random_account_id(),
    "target": _random_account_id(),
    "amount": _random_amount(),
    # Keep it simple: it's all dollars
    "currency": 'USD'
    }


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode(),
        )

    while True:
        transaction: dict = create_random_transaction()
        producer.send(TRANSACTIONS_TOPIC, value=transaction)
        print(transaction) # DEBUG
        sleep(SLEEP_TIME)

