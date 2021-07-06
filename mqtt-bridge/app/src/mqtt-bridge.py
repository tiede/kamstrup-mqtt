import re
from typing import NamedTuple
import datetime

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = ''
INFLUXDB_USER = ''
INFLUXDB_PASSWORD = ''
INFLUXDB_DATABASE = ''

MQTT_ADDRESS = ''
MQTT_USER = ''
MQTT_PASSWORD = ''
MQTT_TOPIC = '+/+/sensor/+/+'
# floor/room/sensor/sensor_location/measurement - groundfloor/kitchen/sensor/room/temperature
MQTT_REGEX = '([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^/]+)'
MQTT_CLIENT_ID = 'MQTTBridge'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

SensorData = NamedTuple('SensorData', [('floor', str), ('room', str), ('location', str), ('measurement', str), ('value', float)])

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

def _parse_mqtt_message(topic, payload):
    match = re.match(MQTT_REGEX, topic)
    if match:
        floor = match.group(1)
        room = match.group(2)
        sensor_location = match.group(4)
        location = floor + '_' + room + '_' + sensor_location
        measurement = match.group(5)
        
        try:
            float(payload)
        except ValueError:
            print ('Cannot convert value to float: ' + payload)
            return None

        if measurement == 'status':
            return None
        return SensorData(floor, room, location, measurement, float(payload))
    else:
        return None

def _send_sensor_data_to_influxdb(sensor_data):
    json_body = [
        {
            'measurement': sensor_data.measurement,
            'tags': {
                'location': sensor_data.location,
                'floor': sensor_data.floor,
                'room': sensor_data.room
            },
            'fields': {
                'value': sensor_data.value
            }
        }
    ]
    #print(json_body)
    influxdb_client.write_points(json_body)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    payload = msg.payload.decode('utf-8')
    print(datetime.datetime.now().isoformat() + ': ' + msg.topic + ' | ' + payload)
    sensor_data = _parse_mqtt_message(msg.topic, payload)
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(sensor_data)

def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)

def main():
    _init_influxdb_database()

    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()
