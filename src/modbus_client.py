import json
from paho.mqtt import client as mqtt_client
import pymodbus.client as modbusClient
import logging
import asyncio
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

def publish_mqtt(payload):
    mqtt_broker = os.getenv('MQTT_HOST')
    mqtt_port = int(os.getenv('MQTT_PORT'))
    mqtt_topic = os.getenv('MQTT_TOPIC')
    mqtt_username = os.getenv('MQTT_USER')
    mqtt_password = os.getenv('MQTT_PASSWORD')
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info(f"Connected to MQTT Broker! {mqtt_broker}:{mqtt_port}")
        else:
            logging.error("Failed to connect, return code %d\n", rc)
    try:
        # Set Connecting Client ID
        client = mqtt_client.Client()
        client.username_pw_set(mqtt_username, mqtt_password)
        client.on_connect = on_connect
        client.connect(host=mqtt_broker, port=mqtt_port)
    
        client.loop_start()
        client.publish(mqtt_topic, payload)
        client.loop_stop()
        #logging.info(f"Published to MQTT topic `{mqtt_topic}`")
    except Exception as e:
        logging.error(f"Error publishing to MQTT: {e}")

async def modbus_client(host, port, timeout, retries):
    client = modbusClient.AsyncModbusTcpClient(
        host=host,
        port=port,
        framer='socket',
        timeout=timeout,
        retries=retries,
        reconnect_delay=1,
        reconnect_delay_max=10,
    )
    await client.connect()
    if client.connected :
        #logging.info(f"Connected modbus {host}:{port}")
        return client
    else:
        #logging.error(f"Modbus server unable to connect {host}:{port}")
        return None
        

async def modbus_read(config):
    dt = datetime.now()
    ts = datetime.timestamp(dt)

    modbus_data = {
        "station_name": config["station_name"],
        "timestamp": ts,
        "gateway_info": []
    }

    for gateway_info in config["gateway_info"]:
        host_name = gateway_info["host_name"]
        port = gateway_info["port"]
        timeout = gateway_info["timeout"]/1000

        try:
            client = await modbus_client(host_name, port, timeout, 3)
            gateway_status = True
        except Exception as e:
            logging.error(f"Error connect modbus: {e}")
            gateway_status = False
            pass

        gateway_data = {
            "gateway_name": gateway_info["gateway_name"],
            "host_name": host_name,
            "port": port,
            "status" :gateway_status,
            "device_info": []
        }
        try:
            for device_info in gateway_info["device_info"]:
                device_id = device_info["device_id"]
                group_name = device_info["group_name"]
                device_data = {
                    "device_name": device_info["device_name"],
                    "group_name": group_name,
                    "device_id": device_id,
                    "status" : True,
                    "value_info": []
                }

                for value_info in device_info["value_info"]:
                    marge_address = value_info["marge_address"]
                    fc = value_info["fc"]
                    if marge_address:
                        if client :
                            try:
                                if fc == 3:
                                    await asyncio.sleep(1)
                                    results = await client.read_holding_registers(  value_info["address"][0], 
                                                                                    value_info["quantity"],
                                                                                    slave=device_info["unit_id"])                             
                                elif fc == 4:
                                    await asyncio.sleep(1)
                                    results = await client.read_input_registers(value_info["address"][0], 
                                                                                value_info["quantity"],
                                                                                slave=device_info["unit_id"])  
                                else:
                                    logging.error(f"Error modbus function: {fc}")

                                if results.isError():
                                    device_data["status"] = False
                                    for i in range(len(value_info["value_name"])):
                                        unit = value_info["unit"][i]
                                        value_name = value_info["value_name"][i]
                                        value_data = {
                                            "value_name": value_name,
                                            "value": None,
                                            "unit": unit,
                                        }
                                        device_data["value_info"].append(value_data)
                                else :
                                    device_data["status"] = True
                                    for i in range(len(value_info["value_name"])):
                                        data_type = value_info["data_type"][i].lower()
                                        byte_order = 'big' if 'be' in data_type else 'little'
                                        element_size = 4 if '32' in data_type else 2
                                        word_size = 2 if '32' in data_type else 1
                                        result = results.registers[
                                                    (value_info["address"][i]):(value_info["address"][i] + word_size)]
                                        data_bytes = b''.join(
                                            [x.to_bytes(element_size, byteorder=byte_order) for x in result])
                                        response_data = int.from_bytes(data_bytes, byteorder=byte_order, signed=False)

                                        scaling = value_info["scaling"][i]
                                        scaling_direction = value_info["scaling_direction"][i]
                                        calibrate = value_info["calibrate"][i]
                                        unit = value_info["unit"][i]
                                        value_name = value_info["value_name"][i]
                                        
                                        if scaling_direction:
                                            formatted_data = round(((response_data * scaling) + calibrate), 3)
                                        else:
                                            formatted_data = round(((response_data / scaling) + calibrate), 3)

                                        value_data = {
                                            "value_name": value_name,
                                            "value": formatted_data,
                                            "unit": unit,
                                        }
                                        device_data["value_info"].append(value_data)

                            except Exception as e:
                                device_data["status"] = False
                                #logging.error(f"Read modbus missing {value_info["value_name"]}: {e}")
                                for i in range(len(value_info["value_name"])):
                                    unit = value_info["unit"][i]
                                    value_name = value_info["value_name"][i]
                                    value_data = {
                                        "value_name": value_name,
                                        "value": None,
                                        "unit": unit,
                                    }
                                    device_data["value_info"].append(value_data)
                                pass
                        else:
                            device_data["status"] = False
                            for i in range(len(value_info["value_name"])):
                                unit = value_info["unit"][i]
                                value_name = value_info["value_name"][i]
                                value_data = {
                                    "value_name": value_name,
                                    "value": None,
                                    "unit": unit,
                                }
                                device_data["value_info"].append(value_data)
                    else:
                        if client :
                            try:
                                if fc == 3:
                                    await asyncio.sleep(1)
                                    if client :
                                        result = await client.read_holding_registers(   address=value_info["address"],
                                                                                        count=value_info["quantity"],
                                                                                        slave=device_info["unit_id"])
                                        #print(f"{config["station_name"]}:{device_info["device_name"]}:{value_name}:{result.registers}")
                                elif fc == 4:
                                    await asyncio.sleep(1)
                                    if client :
                                        result = await client.read_input_registers( address=value_info["address"],
                                                                                    count=value_info["quantity"],
                                                                                    slave=device_info["unit_id"])
                                else:
                                    logging.error(f"Error modbus function: {fc}")

                                if result.isError():
                                    device_data["status"] = False
                                    value_name = value_info["value_name"]
                                    unit = value_info["unit"]
                                    value_data = {
                                        "value_name": value_name,
                                        "value": None,
                                        "unit": unit,
                                    }
                                    device_data["value_info"].append(value_data)
                                else :
                                    device_data["status"] = True
                                    value_name = value_info["value_name"]
                                    data_type = value_info["data_type"].lower()
                                    byte_order = 'big' if 'be' in data_type else 'little'
                                    element_size = 4 if '32' in data_type else 2
                                    data_bytes = b''.join(
                                        [x.to_bytes(element_size, byteorder=byte_order) for x in result.registers])
                                    response_data = int.from_bytes(data_bytes, byteorder=byte_order, signed=False)

                                    scaling = value_info["scaling"]
                                    scaling_direction = value_info["scaling_direction"]
                                    calibrate = value_info["calibrate"]
                                    unit = value_info["unit"]

                                    if scaling_direction:
                                        formatted_data = round(((response_data * scaling) + calibrate), 3)
                                    else:
                                        formatted_data = round(((response_data / scaling) + calibrate), 3)

                                    value_data = {
                                        "value_name": value_name,
                                        "value": formatted_data,
                                        "unit": unit,
                                    }
                                    device_data["value_info"].append(value_data)

                            except Exception as e:
                                device_data["status"] = False
                                #logging.error(f"Read modbus missing {value_info["value_name"]}: {e}")
                                value_name = value_info["value_name"]
                                unit = value_info["unit"]
                                value_data = {
                                    "value_name": value_name,
                                    "value": None,
                                    "unit": unit,
                                }
                                device_data["value_info"].append(value_data)
                                pass
                        else:
                            device_data["status"] = False
                            value_name = value_info["value_name"]
                            unit = value_info["unit"]
                            value_data = {
                                "value_name": value_name,
                                "value": None,
                                "unit": unit,
                            }
                            device_data["value_info"].append(value_data)
                gateway_data["device_info"].append(device_data)
        except Exception as e:
            logging.error(f"Modbus config missing : {e}")
            pass
            

    modbus_data["gateway_info"].append(gateway_data)
    if client:
        client.close()
    return modbus_data

def writefile(filename, json_file):
    with open(filename, "w") as outfile:
        outfile.write(json_file)


def readfile(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)

async def read_modbus():
    modbus_config = f'{os.getcwd()}/modbus_meta.json'
    stations = readfile(modbus_config)
    modbus_data_list = []
    
    tasks = [modbus_read(station) for station in stations]
    modbus_data_list = await asyncio.gather(*tasks) #multiple-tasks by station

    filename = f"{os.getcwd()}/modbus_json_list.json"
    modbus_json_out = json.dumps(modbus_data_list, indent=2)
    writefile(filename, modbus_json_out)
    publish_mqtt(modbus_json_out)
    #logging.info(modbus_json_out)

def write_modbus():
    with open(f'{os.getcwd()}/mobus_command.json', 'r') as file:
        data = json.load(file)
        gateway_info = data[0]['gateway_info'][0]
        device_info = gateway_info['device_info'][0]
        value_info = device_info['value_info'][0]
        value_name = value_info['value_name']
        value = value_info['value']
        print(f"value_name:{value_name} value:{value}")
    #print(json.dumps(original_value_info, sort_keys=True, indent=4))
        


def service_read():
    asyncio.run(read_modbus())

def service_write():
    write_modbus()


if __name__ == '__main__':
    write_modbus()