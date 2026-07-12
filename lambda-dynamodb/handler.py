"""Task 2.5 (bonus): reads new validated events from Snowflake and writes
them to DynamoDB for Grafana's live sensor panel.

Originally written against AWS Timestream per the brief, but Timestream for
LiveAnalytics is closed to new AWS accounts ("Only existing Timestream for
LiveAnalytics customers can access the service", HandlerErrorCode:
GeneralServiceException - confirmed via a live deploy attempt). DynamoDB with
device_id as partition key and event_time (epoch millis) as sort key gives
the same per-device time-range query pattern a dashboard needs.

Runs on an EventBridge schedule (every 1 minute). A dedicated watermark item
(device_id="__watermark__") tracks the last-processed event_ts so each run
only reads new rows - no separate state store needed for a hackathon volume
of data. Snowflake credentials come from Secrets Manager (never hardcoded);
this function needs public internet (Snowflake is a regional SaaS endpoint),
so it is deliberately NOT VPC-attached.
"""
import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import snowflake.connector

TABLE_NAME = os.environ["DYNAMODB_TABLE"]
SNOWFLAKE_SECRET_ARN = os.environ["SNOWFLAKE_SECRET_ARN"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

WATERMARK_KEY = "__watermark__"

_table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
_secrets = boto3.client("secretsmanager", region_name=REGION)


def _snowflake_creds() -> dict:
    secret = _secrets.get_secret_value(SecretId=SNOWFLAKE_SECRET_ARN)
    return json.loads(secret["SecretString"])


def _get_watermark() -> str:
    """Returns a timezone-naive ISO timestamp string: last event_ts already
    written, or 24h ago if this is the first run. Naive (no +00:00 suffix)
    because clean.iot_validated.event_ts is TIMESTAMP_NTZ - comparing it
    against a timezone-aware literal lets Snowflake's session timezone
    (not necessarily UTC) shift the comparison, which silently matched zero
    rows on the very first run despite 75 rows well within the 24h window
    (confirmed at runtime)."""
    resp = _table.get_item(Key={"device_id": WATERMARK_KEY, "event_time": 0})
    item = resp.get("Item")
    if item and "last_event_ts" in item:
        return item["last_event_ts"]
    return (datetime.now(timezone.utc) - timedelta(hours=24)).replace(tzinfo=None).isoformat()


def handler(event, context):
    creds = _snowflake_creds()
    conn = snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        role=creds.get("role", "ACCOUNTADMIN"),
        warehouse=creds.get("warehouse", "COMPUTE_WH"),
        database="HACKATHON_IOT",
        schema="CLEAN",
    )
    watermark = _get_watermark()

    cur = conn.cursor()
    try:
        cur.execute(
            "select device_id, latitude, longitude, event_ts "
            "from clean.iot_validated where event_ts > %s order by event_ts",
            (watermark,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        return {"written": 0}

    max_event_ts = watermark
    with _table.batch_writer() as batch:
        for device_id, latitude, longitude, event_ts in rows:
            event_ts_iso = event_ts.isoformat()
            batch.put_item(
                Item={
                    "device_id": str(device_id),
                    "event_time": int(event_ts.timestamp() * 1000),
                    "latitude": Decimal(str(latitude)),
                    "longitude": Decimal(str(longitude)),
                    "event_ts": event_ts_iso,
                }
            )
            if event_ts_iso > max_event_ts:
                max_event_ts = event_ts_iso

    _table.put_item(Item={"device_id": WATERMARK_KEY, "event_time": 0, "last_event_ts": max_event_ts})
    return {"written": len(rows)}
