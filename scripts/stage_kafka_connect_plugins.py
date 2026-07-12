"""Task 1.3: stages the real Kafka Connect plugin JARs that KafkaConnectStack's
CfnCustomPlugin resources reference (Confluent JDBC Sink + PostgreSQL JDBC
driver merged into one ZIP, and the Confluent S3 Sink connector as-is), and
uploads them to the plugins S3 bucket created by S3BackupStack.

Must run once from a machine with real internet access (this sandbox's
target VPC intentionally has none) BEFORE `cdk deploy IotHackKafkaConnectStack`,
since MSK Connect validates the S3 object exists when the CfnCustomPlugin is
created.

Usage: python stage_kafka_connect_plugins.py <plugins-bucket-name> [region]
"""
import io
import sys
import urllib.request
import zipfile

import boto3

JDBC_CONNECTOR_URL = (
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/"
    "versions/10.9.6/confluentinc-kafka-connect-jdbc-10.9.6.zip"
)
S3_CONNECTOR_URL = (
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/"
    "versions/12.1.7/confluentinc-kafka-connect-s3-12.1.7.zip"
)
POSTGRES_DRIVER_URL = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar"
POSTGRES_DRIVER_FILENAME = "postgresql-42.7.13.jar"


def download(url: str) -> bytes:
    print(f"downloading {url}")
    with urllib.request.urlopen(url) as resp:
        return resp.read()


def build_jdbc_plugin_zip() -> bytes:
    """Confluent's JDBC connector zip doesn't bundle a Postgres driver (only
    a couple of others), so the driver JAR is merged into its lib/ folder.
    """
    jdbc_zip_bytes = download(JDBC_CONNECTOR_URL)
    pg_driver_bytes = download(POSTGRES_DRIVER_URL)

    src = zipfile.ZipFile(io.BytesIO(jdbc_zip_bytes))
    lib_entries = [n for n in src.namelist() if "/lib/" in n]
    if not lib_entries:
        raise RuntimeError("could not find a lib/ directory inside the JDBC connector zip")
    sample = lib_entries[0]
    lib_prefix = sample[: sample.index("/lib/") + len("/lib/")]
    print(f"merging {POSTGRES_DRIVER_FILENAME} into {lib_prefix}")

    out_buf = io.BytesIO()
    with zipfile.ZipFile(out_buf, "w", zipfile.ZIP_DEFLATED) as dst:
        for item in src.infolist():
            dst.writestr(item, src.read(item.filename))
        dst.writestr(f"{lib_prefix}{POSTGRES_DRIVER_FILENAME}", pg_driver_bytes)
    return out_buf.getvalue()


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: python stage_kafka_connect_plugins.py <plugins-bucket-name> [region]")
    bucket = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) > 2 else "us-east-1"

    jdbc_zip = build_jdbc_plugin_zip()
    s3_zip = download(S3_CONNECTOR_URL)

    s3 = boto3.client("s3", region_name=region)
    print(f"uploading jdbc-sink-connector.zip ({len(jdbc_zip):,} bytes) to s3://{bucket}/")
    s3.put_object(Bucket=bucket, Key="jdbc-sink-connector.zip", Body=jdbc_zip)
    print(f"uploading s3-sink-connector.zip ({len(s3_zip):,} bytes) to s3://{bucket}/")
    s3.put_object(Bucket=bucket, Key="s3-sink-connector.zip", Body=s3_zip)
    print("done")


if __name__ == "__main__":
    main()
