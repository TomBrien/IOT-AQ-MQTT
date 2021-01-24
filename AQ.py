import logging
import os
import signal
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

import coloredlogs
import numpy as np
import paho.mqtt.client as mqtt
import yaml
from bme280 import BME280
from pms5003 import PMS5003, ChecksumMismatchError
from sgp30 import SGP30
from smbus2 import SMBus

coloredlogs.install(level="DEBUG")

logger = logging.getLogger(__name__)

if not os.path.exists(os.path.join(os.path.dirname(__file__), "logs")):
    os.makedirs(os.path.join(os.path.dirname(__file__), "logs"))

fh = RotatingFileHandler(
    os.path.join(os.path.dirname(__file__), "logs", "log.txt"),
    maxBytes=2.0 ** 20,
    backupCount=10,
)
fh.setLevel(logging.INFO)
fh.setFormatter(
    logging.Formatter("%(asctime)s [%(process)d] %(levelname)s: %(message)s")
)
logger.addHandler(fh)


def full_topic(topic: str, config: dict) -> str:
    return f"{config['topic']['root']}/{config['topic']['location']}/{topic}"


def term_signal_handler(signal, frame):
    logger.info("Received kill signal, trying to shutdown gracefully")
    raise SystemExit


def on_connect(client, userdata, flags, rc) -> None:
    if rc == 0:
        logger.info(
            "Connected successfully to "
            f"{client.socket().getpeername()[0]}:{client.socket().getpeername()[1]}"
        )
    else:
        logger.warning(
            "Connection refused by "
            f"{client.socket().getpeername()[0]}:{client.socket().getpeername()[1]} "
            f"with response code {rc}"
        )


def on_disconnect(client, userdata, rc) -> None:
    if rc == 0:
        logger.info("Disconnected cleanly")
    else:
        logger.warning("Unexpected disconnect")


# Register kill signal handler
signal.signal(signal.SIGTERM, term_signal_handler)

logger.info("Starting task")

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

logger.info("Attaching paho.mqtt log listener")

client.enable_logger(logger=logger)

client.on_connect = on_connect
client._on_disconnect = on_disconnect

client.connect_async(
    config["broker"]["address"], config["broker"]["port"], keepalive=90
)

logger.info("Starting MQTT loop thread")
client.loop_start()

logger.info("Awaiting MQTT Connection")

# Await connection OK
while not client.is_connected():
    time.sleep(1)

last_boot = datetime.now().isoformat(timespec="seconds")

client.publish(full_topic("last_start", config), last_boot)

logger.info("Warming up sensors, this should take 3 minutes and 15 seconds")
# Warm up sensors
i = 0
sgp30.start_measurement()  # Will take 15 seconds

while i < 180:
    _ = pms5003.read()
    _ = bme280.get_temperature()
    _ = bme280.get_pressure()
    _ = bme280.get_humidity()
    _ = sgp30.get_air_quality()
    time.sleep(1)
    i += 1

logger.info("Warm up finished, entering main loop")

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

run = True

while run:
    try:
        pm = np.append(pm, np.array([list(pms5003.read().data)[:12]]), axis=0)

        temperature = np.append(temperature, bme280.get_temperature())
        pressure = np.append(pressure, bme280.get_pressure())
        humidity = np.append(humidity, bme280.get_humidity())

        gas = sgp30.get_air_quality()
        eCO2 = np.append(eCO2, gas.equivalent_co2)
        VOC = np.append(VOC, gas.total_voc)

        # Send data to broker ever publish_interval
        if datetime.now().timestamp() - last_publish > publish_interval:
            if client.is_connected():
                logger.info("Sending data to broker")
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

            last_publish = datetime.now().timestamp()  # Update regardless of success

            # Empty Arrays
            pm = np.empty((0, 12))
            temperature = np.array([])
            pressure = np.array([])
            humidity = np.array([])
            eCO2 = np.array([])
            VOC = np.array([])

        time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("Got Ctrl+C")
        run = False
    except ChecksumMismatchError:
        logger.warning(
            "Caught PMS 5003 Checksum Error, ditching current data and resetting the clock."
        )
        last_publish = datetime.now().timestamp()  # Clean house on checksum error
        pm = np.empty((0, 12))
        temperature = np.array([])
        pressure = np.array([])
        humidity = np.array([])
        eCO2 = np.array([])
        VOC = np.array([])
    except SystemExit:
        # Close gracefull from sys.exit
        run = False
    except:
        logger.critical(
            f"Unhandled Exception Received: {sys.exc_info()[0]}, "
            f"from line number {sys.exc_info()[2].tb_lineno}"
        )
        logger.critical("Will now enter finally clean up and exit")
        run = False
    finally:
        logger.info("Arrived in finally statement")
        logger.info("Stopping MQTT loop thread")
        client.loop_stop()
        time.sleep(1)
        logger.info("Disconnecting from broker")
        client.disconnect()
