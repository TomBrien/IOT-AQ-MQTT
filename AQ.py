import os
import sys
import time
from datetime import datetime

import numpy as np
import paho.mqtt.client as mqtt
import yaml
from bme280 import BME280
from pms5003 import PMS5003
from sgp30 import SGP30
from smbus2 import SMBus


def full_topic(topic: str, config: dict) -> str:
    return f"{config['topic']['root']}/{config['topic']['location']}/{topic}"


# Create objects
pms5003 = PMS5003(device="/dev/ttyAMA0", baudrate=9600, pin_enable=22, pin_reset=27)

sgp30 = SGP30()

bus = SMBus(1)
bme280 = BME280(i2c_dev=bus)

# Get config
os.chdir(os.path.dirname(__file__))
with open("config.yaml", "r") as fi:
    config = yaml.load(fi, Loader=yaml.Loader)

# Create mqtt client
client = mqtt.Client(f"AQ_{config['topic']['location']}")
if "user" in config["broker"].keys() and "password" in config["broker"].keys():
    client.username_pw_set(config["broker"]["user"], config["broker"]["password"])

last_boot = datetime.now().isoformat(timespec="seconds")

while True:
    conn = client.connect(config["broker"]["address"], config["broker"]["port"])

    if conn == 0:  # OK
        client.publish(
            full_topic("last_start", config),
            last_boot,
        )
        client.disconnect()
        break
    elif conn == 3:  # No response
        time.sleep(60)
    else:  # Authentication problem, this isn't going to get better, exit
        sys.exit(conn)

i = 0

sgp30.start_measurement()  # Will take 15 seconds

while i < 180:  # Warm up sensors
    _ = pms5003.read()
    _ = bme280.get_temperature()
    _ = bme280.get_pressure()
    _ = bme280.get_humidity()
    _ = sgp30.get_air_quality()
    time.sleep(1)
    i += 1

last_publish = datetime.now().timestamp()
if "publish_interval" in config.keys():
    publish_interval = config["publish_interval"]
else:
    publish_interval = 60

pm = np.empty((0, 12))
temperature = np.array([])
pressure = np.array([])
humidity = np.array([])
eCO2 = np.array([])
VOC = np.array([])


while True:
    pm = np.append(pm, np.array([list(pms5003.read().data)[:12]]), axis=0)

    temperature = np.append(temperature, bme280.get_temperature())
    pressure = np.append(pressure, bme280.get_pressure())
    humidity = np.append(humidity, bme280.get_humidity())

    gas = sgp30.get_air_quality()
    eCO2 = np.append(eCO2, gas.equivalent_co2)
    VOC = np.append(VOC, gas.total_voc)

    # Send data to broker ever publish_interval
    if datetime.now().timestamp() - last_publish > publish_interval:
        conn = client.reconnect()
        if conn == 0:  # OK
            client.publish(
                full_topic("pm1.0_ug", config),
                f"{np.mean(pm[:, 3]):.1f}",
            )
            client.publish(
                full_topic("pm2.5_ug", config),
                f"{np.mean(pm[:, 4]):.1f}",
            )
            client.publish(
                full_topic("pm10_ug", config),
                f"{np.mean(pm[:, 5]):.1f}",
            )
            client.publish(
                full_topic("pm_gt_0.3", config),
                f"{np.mean(pm[:, 6]):.1f}",
            )
            client.publish(
                full_topic("pm_gt_0.5", config),
                f"{np.mean(pm[:, 7]):.1f}",
            )
            client.publish(
                full_topic("pm_gt_1.0", config),
                f"{np.mean(pm[:, 8]):.1f}",
            )
            client.publish(
                full_topic("pm_gt_2.5", config),
                f"{np.mean(pm[:, 9]):.1f}",
            )
            client.publish(
                full_topic("pm_gt_5.0", config),
                f"{np.mean(pm[:, 10]):.1f}",
            )
            client.publish(
                full_topic("pm_gt_10.0", config),
                f"{np.mean(pm[:, 11]):.1f}",
            )
            client.publish(
                full_topic("temperature", config),
                f"{np.mean(temperature):.2f}",
            )
            client.publish(
                full_topic("pressure", config),
                f"{np.mean(pressure):.2f}",
            )
            client.publish(
                full_topic("humidity", config),
                f"{np.mean(humidity):.1f}",
            )
            client.publish(
                full_topic("eCO2", config),
                f"{np.mean(eCO2):.1f}",
            )
            client.publish(
                full_topic("VOC", config),
                f"{np.mean(VOC):.1f}",
            )
            client.publish(
                full_topic("last_start", config),
                last_boot,
            )
            client.disconnect()
        elif conn == 3:  # No response
            pass
        else:  # Authentication problem, this isn't going to get better, exit
            sys.exit(conn)

        last_publish = datetime.now().timestamp()  # Update regardless of success

        # Empty Arrays
        pm = np.empty((0, 12))
        temperature = np.array([])
        pressure = np.array([])
        humidity = np.array([])
        eCO2 = np.array([])
        VOC = np.array([])

    time.sleep(1)
