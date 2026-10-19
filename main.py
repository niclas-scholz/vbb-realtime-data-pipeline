import json
import os
import shutil
from pathlib import Path

import requests
from confluent_kafka import OFFSET_END, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "gtfs-rt"
SESSION_PATHS = (
    Path("data/checkpoints"),
    Path("data/bronze"),
    Path("data/silver"),
)


def reset_previous_session() -> None:
    # Topic, checkpoints and outputs must go together: a kept checkpoint would
    # skip the new offsets, kept outputs would keep the previous session.
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    topics = admin.list_topics(timeout=10).topics
    if TOPIC in topics:
        # OFFSET_END (-1) deletes every record up to the high watermark.
        futures = admin.delete_records(
            [
                TopicPartition(TOPIC, partition, OFFSET_END)
                for partition in topics[TOPIC].partitions
            ]
        )
        for future in futures.values():
            future.result()

    for path in SESSION_PATHS:
        if path.exists():
            shutil.rmtree(path)


def main():
    load_dotenv()
    user_agent: str = os.environ.get("VBB_USER_AGENT")

    r = requests.get(
        "https://production.gtfsrt.vbb.de/data",
        headers={"User-Agent": user_agent},
        timeout=10,
    )
    print(r.status_code, len(r.content))
    # Write only after the status is checked: on 429/5xx the body is an error
    # page, and saving it would destroy the last good copy of the feed.
    r.raise_for_status()

    # Parse before resetting: a 200 error page must not delete the last good session.
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    data = MessageToDict(feed)

    reset_previous_session()

    with open("data/gtfs-rt.pb", "wb") as f:
        f.write(r.content)

    with open("data/gtfs-rt.json", "w") as f:
        json.dump(data, f, indent=2)

    p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    entities = data.get("entity", [])
    for e in entities:
        p.produce(TOPIC, json.dumps(e).encode("utf-8"))

    # flush() returns the number of messages still queued, i.e. never delivered.
    # Bound the wait: a dead broker must fail this run instead of hanging it.
    undelivered = p.flush(10)
    if undelivered:
        raise RuntimeError(
            f"{undelivered}/{len(entities)} messages not delivered to Kafka"
        )

    print(f"published {len(entities)} entities to gtfs-rt")


if __name__ == "__main__":
    main()
