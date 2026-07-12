"""Task 1.2: lightweight equivalent of the AWS IoT Device Simulator.

The official AWS Solutions Library "IoT Device Simulator" is its own
20-30 minute multi-stack CloudFormation deployment with a web console -
out of scope for a same-day build. This script is a swap-compatible
substitute: it publishes the identical geoLocation shape
({device_id, latitude, longitude, timestamp}) from 5+ concurrent simulated
devices over MQTT to AWS IoT Core, via per-device X.509 certs created by
provision_things.sh.

Usage:
    python device_simulator.py --endpoint <iot-ats-endpoint> --devices 5 \
        --interval 5 --duration 120
"""
import argparse
import json
import ssl
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from geo_template import GeoLocationDevice

CERTS_DIR = Path(__file__).resolve().parent / "certs"
ROOT_CA = CERTS_DIR / "AmazonRootCA1.pem"


def run_device(device_id: str, endpoint: str, interval: float, stop_event: threading.Event):
    device_dir = CERTS_DIR / device_id
    cert_file = device_dir / "cert.pem.crt"
    key_file = device_dir / "private.pem.key"

    client = mqtt.Client(client_id=device_id)
    client.tls_set(
        ca_certs=str(ROOT_CA),
        certfile=str(cert_file),
        keyfile=str(key_file),
        tls_version=ssl.PROTOCOL_TLSv1_2,
    )
    client.connect(endpoint, 8883, keepalive=60)
    client.loop_start()

    device = GeoLocationDevice(device_id)
    topic = f"iot/{device_id}/geolocation"

    print(f"[{device_id}] connected, publishing to {topic} every {interval}s")
    try:
        while not stop_event.is_set():
            reading = device.next_reading()
            client.publish(topic, json.dumps(reading), qos=1)
            print(f"[{device_id}] {reading}")
            stop_event.wait(interval)
    finally:
        client.loop_stop()
        client.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Simulate 5+ IoT geoLocation devices over MQTT")
    parser.add_argument("--endpoint", required=True, help="AWS IoT Core ATS endpoint (aws iot describe-endpoint)")
    parser.add_argument("--devices", type=int, default=5, help="number of simulated devices (>=5 per Task 1.2)")
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between readings per device")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run, 0 = run until Ctrl+C")
    args = parser.parse_args()

    if args.devices < 5:
        raise SystemExit("Task 1.2 requires 5+ virtual devices; --devices must be >= 5")

    stop_event = threading.Event()
    threads = []
    for i in range(1, args.devices + 1):
        device_id = f"device-{i:03d}"
        t = threading.Thread(target=run_device, args=(device_id, args.endpoint, args.interval, stop_event), daemon=True)
        t.start()
        threads.append(t)

    try:
        if args.duration > 0:
            time.sleep(args.duration)
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=5)


if __name__ == "__main__":
    main()
