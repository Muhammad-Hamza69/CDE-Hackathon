"""Task 2.2: creates the Snowflake objects the pipeline needs and a
key-pair-authenticated service user for the Snowflake Kafka Connector
(Task 2.2 requires key-pair auth, not a password).

Reads the human operator's own Snowflake login from environment variables
(never hardcoded, never committed) to run the one-time setup DDL as
ACCOUNTADMIN, then:
  1. Generates an RSA key pair locally (private key never leaves this
     machine except via Secrets Manager, which is encrypted at rest).
  2. Registers the public key on a new KAFKA_CONNECTOR_SVC user.
  3. Prints the private key PEM so it can be piped into
     `aws secretsmanager put-secret-value` for the SnowflakeKeypairSecret
     created by SecretsStack - never written to a repo file.

Required env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_ROLE (default ACCOUNTADMIN), SNOWFLAKE_WAREHOUSE (default COMPUTE_WH).

Usage: python setup_snowflake.py > /path/to/gitignored/kafka_connector_key.pem
"""
import json
import os
import sys

import boto3
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

DATABASE = "HACKATHON_IOT"
SCHEMAS = ["RAW", "CLEAN", "ANALYTICS"]
SERVICE_USER = "KAFKA_CONNECTOR_SVC"
SERVICE_ROLE = "KAFKA_CONNECTOR_ROLE"


def generate_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    public_key = private_key.public_key()
    public_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    import base64

    public_key_b64 = base64.b64encode(public_der).decode("utf-8")
    return private_pem, public_key_b64


def main():
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PASSWORD"]
    role = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    private_pem, public_key_b64 = generate_keypair()

    conn = snowflake.connector.connect(
        account=account, user=user, password=password, role=role, warehouse=warehouse
    )
    cur = conn.cursor()
    try:
        print(f"-- creating database/schemas", file=sys.stderr)
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        for schema in SCHEMAS:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{schema}")

        print(f"-- creating service role/user for the Snowflake Kafka Connector", file=sys.stderr)
        cur.execute(f"CREATE ROLE IF NOT EXISTS {SERVICE_ROLE}")
        cur.execute(
            f"CREATE USER IF NOT EXISTS {SERVICE_USER} "
            f"RSA_PUBLIC_KEY='{public_key_b64}' "
            f"DEFAULT_ROLE={SERVICE_ROLE} DEFAULT_WAREHOUSE={warehouse} "
            f"MUST_CHANGE_PASSWORD=FALSE"
        )
        cur.execute(f"ALTER USER {SERVICE_USER} SET RSA_PUBLIC_KEY='{public_key_b64}'")
        cur.execute(f"GRANT ROLE {SERVICE_ROLE} TO USER {SERVICE_USER}")
        cur.execute(f"GRANT USAGE ON DATABASE {DATABASE} TO ROLE {SERVICE_ROLE}")
        cur.execute(f"GRANT USAGE, CREATE TABLE ON SCHEMA {DATABASE}.RAW TO ROLE {SERVICE_ROLE}")
        cur.execute(
            f"GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA {DATABASE}.RAW TO ROLE {SERVICE_ROLE}"
        )
        cur.execute(f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {SERVICE_ROLE}")
        conn.commit()
        print("-- Snowflake setup complete", file=sys.stderr)
    finally:
        cur.close()
        conn.close()

    secret_arn = os.environ.get("SNOWFLAKE_SECRET_ARN")
    if secret_arn:
        region = os.environ.get("AWS_REGION", "us-east-1")
        boto3.client("secretsmanager", region_name=region).put_secret_value(
            SecretId=secret_arn, SecretString=json.dumps({"private_key": private_pem})
        )
        print(f"-- private key pushed to Secrets Manager: {secret_arn}", file=sys.stderr)
    else:
        # Fallback: only the private key goes to stdout, never to a repo file.
        print(private_pem)


if __name__ == "__main__":
    main()
