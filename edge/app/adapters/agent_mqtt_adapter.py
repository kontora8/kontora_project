import logging
import paho.mqtt.client as mqtt
from app.interfaces.agent_gateway import AgentGateway
from app.entities.agent_data import AgentData, GpsData
from app.usecases.data_processing import process_agent_data
from app.interfaces.hub_gateway import HubGateway


class AgentMQTTAdapter(AgentGateway):
    def __init__(self, broker_host: str, broker_port: int, topic: str, hub_gateway: HubGateway, batch_size: int = 10):
        """
        Initializes the AgentMQTTAdapter.

        :param broker_host: The hostname of the MQTT broker.
        :param broker_port: The port of the MQTT broker.
        :param topic: The topic to subscribe to.
        :param hub_gateway: The hub gateway instance to forward processed data.
        :param batch_size: Number of messages to accumulate before processing (currently not used).
        """
        self.batch_size = batch_size
        # MQTT configuration
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.client = mqtt.Client()
        # Hub interface for forwarding data
        self.hub_gateway = hub_gateway

    def on_connect(self, client: mqtt.Client, userdata, flags, rc: int):
        """
        Callback triggered when the MQTT client connects to the broker.
        """
        if rc == 0:
            logging.info("MQTT connection established with %s:%s", self.broker_host, self.broker_port)
            client.subscribe(self.topic)
        else:
            logging.error("MQTT connection failed with code: %s", rc)

    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        """
        Handles incoming MQTT messages: decodes payload, processes sensor data, and forwards to the Hub.
        """
        try:
            payload = msg.payload.decode("utf-8")
            logging.debug("Received payload: %s", payload)
            # Convert JSON payload to an AgentData object
            agent_data = AgentData.model_validate_json(payload, strict=True)
            # Process sensor data to determine the road condition
            processed_data = process_agent_data(agent_data)
            # Forward the processed data to the Hub
            if not self.hub_gateway.save_data(processed_data):
                logging.error("Data forwarding failed: Hub gateway unavailable")
            else:
                logging.info("Processed data successfully sent to Hub")
        except Exception as e:
            logging.exception("Error during message processing:")

    def connect(self):
        """
        Establishes connection to the MQTT broker and sets up callbacks.
        """
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        except Exception as e:
            logging.exception("Failed to connect to MQTT broker at %s:%s", self.broker_host, self.broker_port)

    def start(self):
        """
        Starts the MQTT client loop.
        """
        self.client.loop_start()

    def stop(self):
        """
        Stops the MQTT client loop and disconnects from the broker.
        """
        self.client.loop_stop()
        self.client.disconnect()


# Usage example:
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s"
    )
    broker_host = "localhost"
    broker_port = 1883
    topic = "agent_data_topic"
    # IMPORTANT: Replace HubGateway() with a concrete implementation (e.g., HubMqttAdapter)
    store_gateway = HubGateway()  # Placeholder; should be replaced with an actual implementation
    adapter = AgentMQTTAdapter(broker_host, broker_port, topic, store_gateway)
    adapter.connect()
    adapter.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        adapter.stop()
        logging.info("MQTT Adapter has been stopped.")
