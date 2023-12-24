# Файл содержит в себе структуру Kafka Consumer согласно заданию.
from confluent_kafka import Consumer
import pandas as pd
import time
import sys
import json

name_of_kafkatopic = "IoT_Devices"
value_of_timesleep = 1

print("Starting Kafka-Consumer...")
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "IoT-Devices",
    "auto.offset.reset": "earliest"
})
print("Kafka-Consumer started succesfully!")
print("Connecting to Kafka-Topic...")
consumer.subscribe([name_of_kafkatopic])
print("Successfully Connected!")
print()

try:
    while True:
        messages_list = []
        while True:
            current_message = consumer.poll(timeout=1.0)
            if current_message is None:
                break
            elif not current_message.error():
                value = current_message.value().decode("utf-8")
                data = json.loads(value)
                messages_list.append(data)
                print(f"Received Message: {current_message.value().decode('utf-8')}")
            else:
                print("An Error Occurred While Performing The Operation!")

            print(f"A total of {len(messages_list)} messages received")

        if len(messages_list) > 0:
            messages_data_dataframe = pd.DataFrame(messages_list)
            print()
            print("Average by Sensor Name: ")
            mean_by_sensor_name = messages_data_dataframe.groupby(by='Name', as_index=False)['Value'].mean().rename(columns={'Name': 'Sensor Name'})
            print(mean_by_sensor_name.to_markdown())
            print()
            print("Average by Sensor Type: ")
            mean_by_sensor_type = messages_data_dataframe.groupby(by='MetaData', as_index=False)['Value'].mean().rename(columns={'MetaData': 'Sensor Type'})
            print(mean_by_sensor_type.to_markdown())
            print()
        time.sleep(value_of_timesleep)
except KeyboardInterrupt:
    sys.stderr.write("Aborted by User!\n")
finally:
    consumer.close()
