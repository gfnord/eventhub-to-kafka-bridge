from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore  # noqa
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json import dumps
from dotenv import load_dotenv

import asyncio
import logging
import os


load_dotenv()

# create console handler and set level to debug
logger = logging.getLogger()
logger.propagate = False
consoleHandler = logging.StreamHandler()
logger.setLevel(logging.WARNING)
logger.addHandler(consoleHandler)

# Variables
AZ_CS = os.environ.get('AZURE_STORAGE_CONNECTION_STRING', 'default')
BLOB = os.environ.get('AZURE_STORAGE_BLOB_NAME', 'default')
# Azure Event Hub Connection String
EH_CS = os.environ.get('EVENT_HUB_NS_CONNECTION_STRING', 'default')
EH_NAME = os.environ.get('EVENT_HUB_NAME', 'default')
# Kafka Servers
KAFKA_SERVERS = os.environ.get('KAFKA_SERVERS', 'localhost:9092'
                               ).strip('][').split(', ')

# Connection to Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS,
                         api_version=(0, 11, 5),
                         value_serializer=lambda
                         v: dumps(v).encode('utf-8'))


def kafka_snd(topic, msg):
    future = producer.send(topic, msg)
    # Block for 'synchronous' sends
    try:
        future.get(timeout=10)
    except KafkaError as ex:
        # Decide what to do if produce request failed...
        logger.error(ex)


async def on_event(partition_context, event):
    # Print the event data.
    logger.debug("Received the event: \"{}\" from the partition with "
          "ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))  # noqa
    logger.warning('.')

    # Send message to Kafka
    kafka_snd('data', event.body_as_str(encoding='UTF-8'))

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)


async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(AZ_CS, BLOB)

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(EH_CS, consumer_group="$Default", eventhub_name=EH_NAME, checkpoint_store=checkpoint_store)  # noqa

    async with client:
        # Call the receive method.
        await client.receive(on_event=on_event)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())
