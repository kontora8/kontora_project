import logging
from paho.mqtt import client as mqtt_client
from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.hub_gateway import HubGateway


class HubMqttAdapter(HubGateway):
    def __init__(self, broker: str, port: int, topic: str):
        """
        Initializes the HubMqttAdapter.

        :param broker: The MQTT broker hostname.
        :param port: The MQTT broker port.
        :param topic: The MQTT topic to publish processed data.
        """
        self.broker = broker
        self.port = port
        self.topic = topic
        self.mqtt_client = self._connect_mqtt(broker, port)

    def save_data(self, processed_data: ProcessedAgentData) -> bool:
        """
        Publishes processed road data to the Hub via MQTT.

        :param processed_data: Processed road data to be sent.
        :return: True if the data is published successfully, False otherwise.
        """
        try:
            msg = processed_data.model_dump_json()
            result = self.mqtt_client.publish(self.topic, msg)
            if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
                logging.info("Successfully published message to topic '%s'", self.topic)
                return True
            else:
                logging.error("Failed to publish message to topic '%s'. Status code: %s", self.topic, result.rc)
                return False
        except Exception as e:
            logging.exception("Error occurred while publishing data:")
            return False

    @staticmethod
    def _connect_mqtt(broker: str, port: int):
        """
        Initializes the MQTT client and establishes a connection.

        :param broker: The MQTT broker hostname.
        :param port: The MQTT broker port.
        :return: Connected MQTT client instance.
        """
        logging.info("Connecting to MQTT broker at %s:%s", broker, port)

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logging.info("Connected to MQTT broker at %s:%s", broker, port)
            else:
                logging.error("Failed to connect to MQTT broker at %s:%s with return code %d", broker, port, rc)

        client = mqtt_client.Client()
        client.on_connect = on_connect
        try:
            client.connect(broker, port, keepalive=60)
            client.loop_start()
        except Exception:
            logging.exception("MQTT connection error:")
        return client
