"""Stream synthetic clickstream events into Azure Event Hubs.

    pip install -e ".[streaming]"
    export EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;EntityPath=clickstream"
    python scripts/clickstream_producer.py --file landing/clickstream/batch_002.json --rate 50

The Lakeflow pipeline reads the hub through its Kafka endpoint when
``clickstream_source`` is set to ``eventhubs``.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from azure.eventhub import EventData, EventHubProducerClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="JSON-lines file of clickstream events")
    parser.add_argument("--rate", type=float, default=20.0, help="events per second")
    args = parser.parse_args()

    conn = os.environ["EVENTHUB_CONNECTION_STRING"]  # never hard-code secrets
    producer = EventHubProducerClient.from_connection_string(conn)
    lines = Path(args.file).read_text().splitlines()
    sent = 0
    with producer:
        batch = producer.create_batch()
        for line in lines:
            try:
                batch.add(EventData(line))
            except ValueError:  # batch full
                producer.send_batch(batch)
                batch = producer.create_batch()
                batch.add(EventData(line))
            sent += 1
            if sent % 100 == 0:
                producer.send_batch(batch)
                batch = producer.create_batch()
                print(f"sent {sent}/{len(lines)}")
            time.sleep(1 / args.rate)
        if len(batch):
            producer.send_batch(batch)
    print(f"done: {sent} events")


if __name__ == "__main__":
    main()
