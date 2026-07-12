"""IoT Core -> Kafka MSK bridge.

AWS IoT Core has no native "publish to MSK" rule action, so this Lambda is
the bridge: an IoT Core Topic Rule (SELECT * FROM 'iot/+/geolocation')
invokes it directly with the MQTT message payload as the event, and it
re-publishes that payload onto the MSK topic `iot-events`, authenticating to
the cluster via IAM (no Kafka username/password ever exists).

device_id is used as the partition key so all events for one device stay in
order within the topic.
"""
import json
import logging
import os
from datetime import datetime

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOOTSTRAP_BROKERS = os.environ["MSK_BOOTSTRAP_BROKERS"]
TOPIC = os.environ.get("KAFKA_TOPIC", "iot-events")
REGION = os.environ.get("AWS_REGION", "us-east-1")

# The JDBC sink connector (io.confluent.connect.jdbc, v10.9.6) rejects any
# schemaless (plain Map) record with "requires records with a non-null
# Struct or String value and non-null Struct or String schema" - confirmed
# at runtime regardless of pk.mode. It needs Kafka Connect's standard JSON
# envelope ({"schema": ..., "payload": ...}), not bare JSON, so this wraps
# every event in an explicit struct schema before publishing. `timestamp`
# uses Connect's logical Timestamp type (epoch millis) so the JDBC sink
# binds it as a real SQL timestamp instead of a VARCHAR.
_VALUE_SCHEMA = {
    "type": "struct",
    "optional": False,
    "name": "iot.GeoLocation",
    "fields": [
        {"field": "device_id", "type": "string", "optional": False},
        {"field": "latitude", "type": "double", "optional": False},
        {"field": "longitude", "type": "double", "optional": False},
        {
            "field": "timestamp",
            "type": "int64",
            "optional": False,
            "name": "org.apache.kafka.connect.data.Timestamp",
            "version": 1,
        },
    ],
}

_producer = None


def _to_epoch_millis(timestamp_value) -> int:
    if isinstance(timestamp_value, (int, float)):
        return int(timestamp_value)
    return int(datetime.fromisoformat(str(timestamp_value)).timestamp() * 1000)


class _MskIamTokenProvider:
    """kafka-python calls .token() on every connection/reconnect attempt."""

    def token(self):
        token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        logger.info("initializing KafkaProducer bootstrap=%s topic=%s", BOOTSTRAP_BROKERS, TOPIC)
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_BROKERS.split(","),
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=_MskIamTokenProvider(),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
            linger_ms=50,
            request_timeout_ms=15000,
        )
    return _producer


def handler(event, context):
    device_id = str(event.get("device_id", "unknown"))
    envelope = {
        "schema": _VALUE_SCHEMA,
        "payload": {
            "device_id": device_id,
            "latitude": float(event["latitude"]),
            "longitude": float(event["longitude"]),
            "timestamp": _to_epoch_millis(event["timestamp"]),
        },
    }

    try:
        producer = _get_producer()
        future = producer.send(TOPIC, key=device_id, value=envelope)
        metadata = future.get(timeout=10)
        producer.flush(timeout=10)
    except KafkaError:
        logger.exception("failed to publish device_id=%s payload=%s", device_id, json.dumps(event))
        raise

    logger.info(
        "published device_id=%s partition=%s offset=%s payload=%s",
        device_id,
        metadata.partition,
        metadata.offset,
        json.dumps(event),
    )
    return {"status": "ok", "partition": metadata.partition, "offset": metadata.offset}
