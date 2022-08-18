"""
Start processing only latest records:
$ python consumergroup.py group1 c1
$ python consumergroup.py group1 c2
$ python consumergroup.py group1 c3

Start processing all records in the stream from the beginning:
$ python consumergroup.py group1 c1 --start-from 0
"""
import typer
import random
import time
from walrus import Database
from enum import Enum

BLOCK_TIME = 5000
STREAM_KEY = "app_event"

app = typer.Typer()



class StartFrom(str, Enum):
    beginning = "0"
    latest = "$"


@app.command()
def start(group_id, consumer_id: str, start_from: StartFrom = StartFrom.latest):
    num_received = 0
    num_processed_with_ack = 0
    rdb = Database()
    cg = rdb.consumer_group(group_id, [STREAM_KEY], consumer=consumer_id)
    cg.create()  # Create the consumer group. Default starts from the latest
    if start_from == StartFrom.beginning:
        cg.set_id(start_from)

    while True:
        print("Reading stream...")
        streams = cg.read(1, block=BLOCK_TIME)

        for stream_id, messages in streams:
            for message_id, message in messages:
                try:
                    num_received += 1
                    print(f"processing {stream_id}::{message_id}::{message}")
                    print("temp value", float(message[b"temp"]), float(message[b"temp"]) > 0.5)
                    # simulate processing
                    time.sleep(random.randint(0, 2))
                    # simulate error %50 percent
                    if random.randint(0, 10)> 5:
                        raise(ValueError(f"Consumer Failed while processing: {message_id}"))
                    print(f"finished processing {message_id}")
                    cg.app_event.ack(message_id)
                    num_processed_with_ack +=1
                    print(f"{cg.app_event.key} {cg.app_event.group}, Num Received: {num_received}, Num Processed : {num_processed_with_ack}")
                except Exception as e:
                    print(f"Error occured in processing {message_id}, Exception: {e}" )
                    pass

if __name__ == "__main__":
    app()
