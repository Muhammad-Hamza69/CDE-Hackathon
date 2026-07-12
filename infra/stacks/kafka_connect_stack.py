import json
from pathlib import Path

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kafkaconnect as kafkaconnect
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

from stacks._msk_arn import msk_resource_arn_wildcard

CONNECT_JSON_DIR = Path(__file__).resolve().parents[2] / "kafka-connect"
KAFKA_CONNECT_VERSION = "2.7.1"


class KafkaConnectStack(Stack):
    """MSK Connect: JDBC sink (Task 1.3) + S3 sink (Task 1.3, optional backup).

    Runs in the PUBLIC subnets (locked-down SG, no inbound) so it has real
    internet egress for Phase 2's Snowflake sink connector without paying
    for a NAT Gateway. Custom plugin JARs are staged in S3 beforehand by
    scripts/stage_kafka_connect_plugins.sh (needs real internet access,
    which this sandboxed private VPC intentionally does not have at
    runtime - staging happens from the operator's machine, once).
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        sg_kafka_connect: ec2.SecurityGroup,
        bootstrap_brokers_iam: str,
        msk_cluster_arn: str,
        postgres_private_ip: str,
        postgres_secret: secretsmanager.Secret,
        plugins_bucket: s3.Bucket,
        backup_bucket: s3.Bucket,
        enable_phase2: bool = False,
        snowflake_secret: secretsmanager.Secret | None = None,
        snowflake_account: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        public_subnet_ids = vpc.select_subnets(subnet_type=ec2.SubnetType.PUBLIC).subnet_ids

        # --- Shared IAM role for all connectors in this MSK Connect deployment ---
        connect_role = iam.Role(
            self,
            "KafkaConnectExecutionRole",
            assumed_by=iam.ServicePrincipal("kafkaconnect.amazonaws.com"),
        )
        connect_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:Connect",
                    "kafka-cluster:AlterCluster",
                    "kafka-cluster:DescribeCluster",
                    "kafka-cluster:DescribeClusterDynamicConfiguration",
                ],
                resources=[msk_cluster_arn],
            )
        )
        topic_arn = msk_resource_arn_wildcard(msk_cluster_arn, "topic")
        group_arn = msk_resource_arn_wildcard(msk_cluster_arn, "group")
        connect_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:*Topic*",
                    "kafka-cluster:ReadData",
                    "kafka-cluster:WriteData",
                    "kafka-cluster:DescribeTopicDynamicConfiguration",
                ],
                resources=[topic_arn],
            )
        )
        connect_role.add_to_policy(
            iam.PolicyStatement(
                actions=["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"],
                resources=[group_arn],
            )
        )
        plugins_bucket.grant_read(connect_role)
        backup_bucket.grant_read_write(connect_role)
        postgres_secret.grant_read(connect_role)

        log_group = logs.LogGroup(
            self,
            "KafkaConnectLogGroup",
            log_group_name="/msk-connect/iot-hack",
            removal_policy=RemovalPolicy.DESTROY,
        )

        def custom_plugin(construct_id: str, name: str, s3_key: str) -> kafkaconnect.CfnCustomPlugin:
            return kafkaconnect.CfnCustomPlugin(
                self,
                construct_id,
                name=name,
                content_type="ZIP",
                location=kafkaconnect.CfnCustomPlugin.CustomPluginLocationProperty(
                    s3_location=kafkaconnect.CfnCustomPlugin.S3LocationProperty(
                        bucket_arn=plugins_bucket.bucket_arn,
                        file_key=s3_key,
                    )
                ),
            )

        def kafka_cluster_props() -> kafkaconnect.CfnConnector.KafkaClusterProperty:
            return kafkaconnect.CfnConnector.KafkaClusterProperty(
                apache_kafka_cluster=kafkaconnect.CfnConnector.ApacheKafkaClusterProperty(
                    bootstrap_servers=bootstrap_brokers_iam,
                    vpc=kafkaconnect.CfnConnector.VpcProperty(
                        subnets=public_subnet_ids,
                        security_groups=[sg_kafka_connect.security_group_id],
                    ),
                )
            )

        def small_capacity() -> kafkaconnect.CfnConnector.CapacityProperty:
            return kafkaconnect.CfnConnector.CapacityProperty(
                provisioned_capacity=kafkaconnect.CfnConnector.ProvisionedCapacityProperty(
                    mcu_count=1, worker_count=1
                )
            )

        def make_connector(
            construct_id: str,
            connector_name: str,
            description: str,
            config: dict,
            plugin: kafkaconnect.CfnCustomPlugin,
        ) -> kafkaconnect.CfnConnector:
            connector = kafkaconnect.CfnConnector(
                self,
                construct_id,
                connector_name=connector_name,
                connector_description=description,
                kafka_connect_version=KAFKA_CONNECT_VERSION,
                capacity=small_capacity(),
                connector_configuration=config,
                kafka_cluster=kafka_cluster_props(),
                kafka_cluster_client_authentication=kafkaconnect.CfnConnector.KafkaClusterClientAuthenticationProperty(
                    authentication_type="IAM"
                ),
                kafka_cluster_encryption_in_transit=kafkaconnect.CfnConnector.KafkaClusterEncryptionInTransitProperty(
                    encryption_type="TLS"
                ),
                plugins=[
                    kafkaconnect.CfnConnector.PluginProperty(
                        custom_plugin=kafkaconnect.CfnConnector.CustomPluginProperty(
                            custom_plugin_arn=plugin.attr_custom_plugin_arn,
                            revision=plugin.attr_revision,
                        )
                    )
                ],
                service_execution_role_arn=connect_role.role_arn,
                log_delivery=kafkaconnect.CfnConnector.LogDeliveryProperty(
                    worker_log_delivery=kafkaconnect.CfnConnector.WorkerLogDeliveryProperty(
                        cloud_watch_logs=kafkaconnect.CfnConnector.CloudWatchLogsLogDeliveryProperty(
                            enabled=True, log_group=log_group.log_group_name
                        )
                    )
                ),
            )
            # `service_execution_role_arn=connect_role.role_arn` is just an ARN
            # token - it does NOT make CloudFormation wait for connect_role's
            # inline DefaultPolicy (the kafka-cluster:*/S3/Secrets grants) to
            # finish attaching first. Without this, MSK Connect can try to
            # launch using the role before it has any permissions at all,
            # which fails with a bare "Access denied" (confirmed by deploy).
            default_policy = connect_role.node.try_find_child("DefaultPolicy")
            if default_policy is not None:
                connector.node.add_dependency(default_policy)
            return connector

        # ---------------- JDBC Sink Connector (Task 1.3) ----------------
        jdbc_plugin = custom_plugin("JdbcSinkPlugin", "jdbc-sink-plugin", "jdbc-sink-connector.zip")

        jdbc_config = json.loads((CONNECT_JSON_DIR / "jdbc-sink-postgres.json").read_text())
        jdbc_config["connection.url"] = f"jdbc:postgresql://{postgres_private_ip}:5432/postgres"
        jdbc_config["connection.user"] = postgres_secret.secret_value_from_json("username").unsafe_unwrap()
        jdbc_config["connection.password"] = postgres_secret.secret_value_from_json("password").unsafe_unwrap()

        self.jdbc_sink_connector = make_connector(
            "JdbcSinkConnector",
            "jdbc-sink-postgres",
            "Task 1.3: iot-events topic -> PostgreSQL EC2 (iot.iot_events)",
            jdbc_config,
            jdbc_plugin,
        )

        # ---------------- S3 Sink Connector (Task 1.3, optional backup) ----------------
        s3_plugin = custom_plugin("S3SinkPlugin", "s3-sink-plugin", "s3-sink-connector.zip")

        s3_config = json.loads((CONNECT_JSON_DIR / "s3-sink-backup.json").read_text())
        s3_config["s3.bucket.name"] = backup_bucket.bucket_name
        s3_config["s3.region"] = self.region

        self.s3_sink_connector = make_connector(
            "S3SinkConnector",
            "s3-sink-backup",
            "Task 1.3 (optional): iot-events topic -> S3 backup bucket",
            s3_config,
            s3_plugin,
        )

        # ---------------- Phase 2: Debezium source + Snowflake sink ----------------
        # Added only once Phase 1 is verified end-to-end (per the build plan).
        if enable_phase2:
            if snowflake_secret is None or snowflake_account is None:
                raise ValueError("enable_phase2=True requires snowflake_secret and snowflake_account")

            postgres_secret.grant_read(connect_role)  # already granted above; harmless if repeated
            snowflake_secret.grant_read(connect_role)

            debezium_plugin = custom_plugin(
                "DebeziumSourcePlugin", "debezium-postgres-source-plugin", "debezium-postgres-connector.zip"
            )
            debezium_config = json.loads((CONNECT_JSON_DIR / "debezium-postgres-source.json").read_text())
            debezium_config["database.hostname"] = postgres_private_ip
            debezium_config["database.user"] = postgres_secret.secret_value_from_json("username").unsafe_unwrap()
            debezium_config["database.password"] = postgres_secret.secret_value_from_json("password").unsafe_unwrap()

            self.debezium_source_connector = make_connector(
                "DebeziumSourceConnector",
                "debezium-postgres-source",
                "Task 2.1: PostgreSQL CDC -> cdc.iot.iot_events",
                debezium_config,
                debezium_plugin,
            )

            snowflake_plugin = custom_plugin(
                "SnowflakeSinkPlugin", "snowflake-sink-plugin", "snowflake-kafka-connector.zip"
            )
            snowflake_config = json.loads((CONNECT_JSON_DIR / "snowflake-sink.json").read_text())
            snowflake_config["snowflake.url.name"] = f"{snowflake_account}.snowflakecomputing.com:443"
            snowflake_config["snowflake.private.key"] = snowflake_secret.secret_value_from_json(
                "private_key"
            ).unsafe_unwrap()

            self.snowflake_sink_connector = make_connector(
                "SnowflakeSinkConnector",
                "snowflake-sink",
                "Task 2.2: cdc.iot.iot_events topic -> Snowflake RAW.IOT_EVENTS",
                snowflake_config,
                snowflake_plugin,
            )

        self.connect_role = connect_role
        CfnOutput(self, "KafkaConnectLogGroupName", value=log_group.log_group_name)
