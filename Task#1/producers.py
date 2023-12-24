# Файл содержит в себе структуру Kafka Producer согласно заданию.
from confluent_kafka import Producer
from confluent_kafka import KafkaError
import json
import random
import time

name_of_kafkatopic = "IoT_Devices"
count_of_IoT_devices = 5
value_of_timesleep = 1
password = "admin_1234567890"
messages_sent_successfully = 0

metadata = [('TEMPERATURE', (-100, 150)),
            ('MOISTURE', (0, 100)),
            ('PRESSURE', (400, 800))]


def Report(error_message: KafkaError, message) -> str:
    """
    Reports the Failure or Success of a message delivery.
    :arg:
        error_message (KafkaError): The Error that occurred while message producing.
        message (Actual message): The message that was produced.
    :return:
        report (String): The info about the result of message delivery.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if error_message is not None:
        report = f"Delivery Failed for Message: {message.key()} : {error_message}"
    else:
        report = f"Message: {message.key()} Successfully Produced to Topic: " \
                 f"{message.topic()} Partition: [{message.partition()}] at Offset {message.offset()}"

    return report


producers_list = list()

print("Starting Kafka-Producers...")
for index in range(count_of_IoT_devices):
    producer = Producer({
        'bootstrap.servers': "localhost:9092"
    })
    random_metadata, span = random.choice(metadata)

    print(f"Kafka-Producer with name \"IoT-Device #{index}\" started successfully!")
    producers_list.append((f"IoT-Device #{index}", random_metadata, span, producer))

while True:
    name, random_metadata, span, producer = random.choice(producers_list)

    data = {
        "Name": name,
        "TimeStamp": time.strftime("%H:%M:%S %d-%m-%Y"),
        "MetaData": random_metadata,
        "Value": random.uniform(span[0], span[1])
        }
    
    producer.produce(topic=name_of_kafkatopic,
                     key=str(time.strftime("%H:%M:%S %d-%m-%Y")).encode('utf-8'),
                     value=json.dumps(data, indent=2).encode('utf-8'),
                     on_delivery=Report)
    time.sleep(value_of_timesleep)
    producer.flush()
    messages_sent_successfully += 1
