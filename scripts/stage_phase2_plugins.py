"""Task 2.1/2.2: stages the Debezium PostgreSQL source connector and the
Snowflake Kafka Connector plugin archives that KafkaConnectStack's
enable_phase2=True CfnCustomPlugin resources reference, uploading them to
the plugins S3 bucket created by S3BackupStack.

Must run once from a machine with real internet access (same constraint as
stage_kafka_connect_plugins.py) BEFORE `cdk deploy IotHackKafkaConnectStack`
with enable_phase2=True.

Usage: python stage_phase2_plugins.py <plugins-bucket-name> [region]
"""
import io
import sys
import tarfile
import urllib.request
import zipfile

import boto3

DEBEZIUM_VERSION = "3.6.0.Final"
DEBEZIUM_URL = (
    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/"
    f"{DEBEZIUM_VERSION}/debezium-connector-postgres-{DEBEZIUM_VERSION}-plugin.tar.gz"
)

SNOWFLAKE_VERSION = "4.0.2"
SNOWFLAKE_URL = (
    "https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/"
    f"{SNOWFLAKE_VERSION}/snowflake-kafka-connector-{SNOWFLAKE_VERSION}.jar"
)


def download(url: str) -> bytes:
    print(f"downloading {url}")
    with urllib.request.urlopen(url) as resp:
        return resp.read()


def build_debezium_plugin_zip() -> bytes:
    """MSK Connect custom plugins must be ZIP or JAR; Debezium only publishes
    its self-managed-Connect plugin bundle as a tar.gz, so re-pack it."""
    tar_bytes = download(DEBEZIUM_URL)
    out_buf = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar, \
            zipfile.ZipFile(out_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            zf.writestr(member.name, f.read())
    return out_buf.getvalue()


def build_snowflake_plugin_zip() -> bytes:
    """The Snowflake Kafka Connector ships as a single fat JAR; MSK Connect
    scans all jars found anywhere in a plugin ZIP, so wrapping it as-is is
    sufficient (no separate lib/ layout needed)."""
    jar_bytes = download(SNOWFLAKE_URL)
    out_buf = io.BytesIO()
    with zipfile.ZipFile(out_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"snowflake-kafka-connector-{SNOWFLAKE_VERSION}.jar", jar_bytes)
    return out_buf.getvalue()


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: python stage_phase2_plugins.py <plugins-bucket-name> [region]")
    bucket = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) > 2 else "us-east-1"

    debezium_zip = build_debezium_plugin_zip()
    snowflake_zip = build_snowflake_plugin_zip()

    s3 = boto3.client("s3", region_name=region)
    print(f"uploading debezium-postgres-connector.zip ({len(debezium_zip):,} bytes) to s3://{bucket}/")
    s3.put_object(Bucket=bucket, Key="debezium-postgres-connector.zip", Body=debezium_zip)
    print(f"uploading snowflake-kafka-connector.zip ({len(snowflake_zip):,} bytes) to s3://{bucket}/")
    s3.put_object(Bucket=bucket, Key="snowflake-kafka-connector.zip", Body=snowflake_zip)
    print("done")


if __name__ == "__main__":
    main()
