import paho.mqtt.client as mqtt
import json
import os
from dotenv import load_dotenv

load_dotenv()

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    mqtt_topic = 'wbm_v1/config'
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    try:
        received_data = json.loads(msg.payload.decode('utf-8'))

        # Write the received JSON data to a new file
        with open(f"{os.getcwd()}/modbus_meta_release.json", "w") as outfile:
            json.dump(received_data, outfile, indent=4)

        print("Received data written to modbus_meta.json")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

def update_now():
    mqtt_broker = os.getenv('MQTT_HOST')
    mqtt_port = int(os.getenv('MQTT_PORT'))
    mqtt_username = os.getenv('MQTT_USER')
    mqtt_password = os.getenv('MQTT_PASSWORD')

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Set the username and password for MQTT authentication
    client.username_pw_set(username=mqtt_username, password=mqtt_password)

    # Connect to the MQTT broker
    client.connect(mqtt_broker, mqtt_port, 60)

    # Start the MQTT loop to handle incoming messages
    client.loop_forever()

if __name__ == "__main__":
    update_now()
