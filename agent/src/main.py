from paho.mqtt import client as mqtt_client
import json
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from file_datasource import FileDatasource
import config


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, topic, datasource, delay):
    datasource.startReading()
    while True:
        time.sleep(delay)
        data = datasource.read()

        acc_msg = json.dumps({
            "x": data.accelerometer.x,
            "y": data.accelerometer.y,
            "z": data.accelerometer.z,
            "timestamp": data.timestamp.isoformat(),
            "user_id": data.user_id
        })
        acc_result = client.publish("accelerometer", acc_msg)

        gps_msg = json.dumps({
            "latitude": data.gps.latitude,
            "longitude": data.gps.longitude,
            "timestamp": data.timestamp.isoformat(),
            "user_id": data.user_id
        })
        gps_result = client.publish("gps", gps_msg)
 
        parking_msg = json.dumps({
            "gps": {
                "latitude": data.parking.gps.latitude,
                "longitude": data.parking.gps.longitude,
            },
            "empty_count": data.parking.empty_count,
            "timestamp": data.timestamp.isoformat(),
            "user_id": data.user_id
        })
        parking_result = client.publish("parking", parking_msg)

        aggregated_msg = AggregatedDataSchema().dumps(data)
        agg_result = client.publish(topic, aggregated_msg)

        if all(r[0] == 0 for r in [acc_result, gps_result, parking_result, agg_result]):
            pass
        else:
            print(f"Failed to send some messages to topics")


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, config.MQTT_TOPIC, datasource, config.DELAY)


if __name__ == "__main__":
    run()
