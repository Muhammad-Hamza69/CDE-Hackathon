#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.network_stack import NetworkStack
from stacks.secrets_stack import SecretsStack
from stacks.msk_stack import MskStack
from stacks.postgres_stack import PostgresStack
from stacks.s3_backup_stack import S3BackupStack
from stacks.kafka_connect_ec2_stack import KafkaConnectEc2Stack
from stacks.iot_bridge_stack import IotBridgeStack
from stacks.dynamodb_stack import DynamoDbStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

# Staged rollout flags (per the build plan: Phase 1 verified end-to-end
# before Phase 2 connectors are added; bonus built last). Set via env var
# once the preceding phase is verified - never hardcoded to True here.
ENABLE_PHASE2 = os.environ.get("ENABLE_PHASE2", "false").lower() == "true"
ENABLE_BONUS = os.environ.get("ENABLE_BONUS", "false").lower() == "true"

network = NetworkStack(app, "IotHackNetworkStack", env=env)

secrets = SecretsStack(app, "IotHackSecretsStack", env=env)

msk = MskStack(
    app,
    "IotHackMskStack",
    vpc=network.vpc,
    sg_msk=network.sg_msk,
    env=env,
)
msk.add_dependency(network)

postgres = PostgresStack(
    app,
    "IotHackPostgresStack",
    vpc=network.vpc,
    sg_postgres=network.sg_postgres,
    sg_bastion=network.sg_bastion,
    postgres_secret=secrets.postgres_secret,
    env=env,
)
postgres.add_dependency(network)
postgres.add_dependency(secrets)

s3_backup = S3BackupStack(app, "IotHackS3BackupStack", env=env)

phase2_kwargs = {}
if ENABLE_PHASE2:
    phase2_kwargs = dict(
        enable_phase2=True,
        snowflake_secret=secrets.snowflake_secret,
        snowflake_account=os.environ["SNOWFLAKE_ACCOUNT"],
    )

kafka_connect = KafkaConnectEc2Stack(
    app,
    "IotHackKafkaConnectStack",
    vpc=network.vpc,
    sg_kafka_connect=network.sg_kafka_connect,
    bootstrap_brokers_iam=msk.bootstrap_brokers_iam,
    msk_cluster_arn=msk.cluster.attr_arn,
    postgres_private_ip=postgres.postgres_instance.instance_private_ip,
    postgres_secret=secrets.postgres_secret,
    plugins_bucket=s3_backup.plugins_bucket,
    backup_bucket=s3_backup.backup_bucket,
    env=env,
    **phase2_kwargs,
)
kafka_connect.add_dependency(msk)
kafka_connect.add_dependency(postgres)
kafka_connect.add_dependency(s3_backup)
kafka_connect.add_dependency(secrets)

iot_bridge = IotBridgeStack(
    app,
    "IotHackIotBridgeStack",
    vpc=network.vpc,
    sg_bridge_lambda=network.sg_bridge_lambda,
    bootstrap_brokers_iam=msk.bootstrap_brokers_iam,
    msk_cluster_arn=msk.cluster.attr_arn,
    env=env,
)
iot_bridge.add_dependency(msk)

if ENABLE_BONUS:
    dynamodb_stack = DynamoDbStack(
        app,
        "IotHackDynamoDbStack",
        snowflake_reader_secret=secrets.snowflake_reader_secret,
        env=env,
    )
    dynamodb_stack.add_dependency(secrets)

app.synth()
