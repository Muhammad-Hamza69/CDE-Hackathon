import json
from pathlib import Path

from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

from stacks._msk_arn import msk_resource_arn_wildcard

CONNECT_JSON_DIR = Path(__file__).resolve().parents[2] / "kafka-connect"
KAFKA_VERSION = "3.7.1"
KAFKA_TGZ = f"kafka_2.13-{KAFKA_VERSION}.tgz"
KAFKA_URL = f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/{KAFKA_TGZ}"
MSK_IAM_AUTH_VERSION = "2.3.7"
MSK_IAM_AUTH_URL = (
    "https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/"
    f"{MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-{MSK_IAM_AUTH_VERSION}-all.jar"
)
# The Snowflake Kafka Connector's fat jar excludes the BouncyCastle FIPS
# provider (licensing reasons), so its JDBC driver throws
# NoClassDefFoundError on org/bouncycastle/... at connector validation time
# unless these are added to the classpath separately (confirmed at runtime).
BC_FIPS_JARS = [
    ("bc-fips", "2.1.2"),
    ("bcpkix-fips", "2.1.11"),
    ("bctls-fips", "2.1.23"),
]


class KafkaConnectEc2Stack(Stack):
    """Self-managed Kafka Connect (Task 1.3), running on a plain EC2 box
    instead of the AWS-managed MSK Connect service.

    Why: MSK Connect's `AWS::KafkaConnect::Connector` resource is gated for
    new AWS accounts - CreateConnector fails with a bare "Access denied...
    reach out to your support representative" even with AdministratorAccess
    on every principal involved (confirmed via CloudTrail: the CDK exec role
    has AdministratorAccess and CustomPlugin creation succeeds fine, only
    Connector creation is denied). That's an account-level service gate, not
    a fixable IAM/CDK bug, so this runs the identical open-source Kafka
    Connect distributed worker directly - same connector plugins (already
    staged in S3 by scripts/stage_kafka_connect_plugins.py +
    stage_phase2_plugins.py), same connector JSON configs, same `iot-events`
    topic, submitted via the worker's own REST API instead of a CFN resource.

    Runs in the public subnet (same reasoning as the original stack: real
    internet egress for the Snowflake API in Phase 2 and for pulling the
    Kafka/plugin binaries, without paying for a NAT Gateway) using the
    existing locked-down `sg_kafka_connect` security group from NetworkStack.
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

        if enable_phase2 and (snowflake_secret is None or snowflake_account is None):
            raise ValueError("enable_phase2=True requires snowflake_secret and snowflake_account")

        public_subnet = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)

        role = iam.Role(
            self,
            "KafkaConnectEc2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")],
        )
        topic_arn = msk_resource_arn_wildcard(msk_cluster_arn, "topic")
        group_arn = msk_resource_arn_wildcard(msk_cluster_arn, "group")
        role.add_to_policy(
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
        role.add_to_policy(
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
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"],
                resources=[group_arn],
            )
        )
        plugins_bucket.grant_read(role)
        backup_bucket.grant_read_write(role)
        postgres_secret.grant_read(role)
        if enable_phase2:
            snowflake_secret.grant_read(role)

        # ---------------- Connector config JSON (same files MSK Connect used) ----------------
        jdbc_config = json.loads((CONNECT_JSON_DIR / "jdbc-sink-postgres.json").read_text())
        jdbc_config["connection.url"] = f"jdbc:postgresql://{postgres_private_ip}:5432/postgres"

        s3_config = json.loads((CONNECT_JSON_DIR / "s3-sink-backup.json").read_text())
        s3_config["s3.bucket.name"] = backup_bucket.bucket_name
        s3_config["s3.region"] = self.region

        connector_files = {
            "jdbc-sink-postgres": (jdbc_config, "postgres"),
            "s3-sink-backup": (s3_config, None),
        }

        if enable_phase2:
            debezium_config = json.loads((CONNECT_JSON_DIR / "debezium-postgres-source.json").read_text())
            debezium_config["database.hostname"] = postgres_private_ip
            connector_files["debezium-postgres-source"] = (debezium_config, "postgres")

            snowflake_config = json.loads((CONNECT_JSON_DIR / "snowflake-sink.json").read_text())
            snowflake_config["snowflake.url.name"] = f"{snowflake_account}.snowflakecomputing.com:443"
            connector_files["snowflake-sink"] = (snowflake_config, "snowflake")

        plugin_zips = ["jdbc-sink-connector.zip", "s3-sink-connector.zip"]
        if enable_phase2:
            plugin_zips += ["debezium-postgres-connector.zip", "snowflake-kafka-connector.zip"]

        # The plain (non-prefixed) sasl.*/security.protocol settings below
        # are enough for the WORKER's own internal clients (admin, and the
        # group-coordination consumer for connect-offsets/configs/status -
        # confirmed working), but per-connector-task producers/consumers
        # (e.g. the JDBC sink's actual data consumer) do NOT reliably
        # inherit them in this Kafka Connect version - confirmed at runtime
        # via a repeating connect/immediately-disconnect loop during the
        # API_VERSIONS handshake, with zero SASL activity ever logged for
        # that specific client. This is a documented AWS MSK + Connect
        # gotcha; the fix is to duplicate the security settings with
        # producer./consumer./admin. prefixes so task-level clients get them
        # explicitly instead of relying on inheritance.
        connect_properties = f"""bootstrap.servers={bootstrap_brokers_iam}
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
producer.security.protocol=SASL_SSL
producer.sasl.mechanism=AWS_MSK_IAM
producer.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
producer.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=AWS_MSK_IAM
consumer.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
consumer.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
admin.security.protocol=SASL_SSL
admin.sasl.mechanism=AWS_MSK_IAM
admin.sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
admin.sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
group.id=iot-hack-connect-cluster
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.topic=connect-offsets
offset.storage.replication.factor=2
config.storage.topic=connect-configs
config.storage.replication.factor=2
status.storage.topic=connect-status
status.storage.replication.factor=2
offset.flush.interval.ms=10000
plugin.path=/opt/kafka-connect-plugins
listeners=http://0.0.0.0:8083
rest.advertised.host.name=localhost
"""

        plugin_fetch_lines = "\n".join(
            f'mkdir -p /opt/kafka-connect-plugins/{zipname[:-4]}\n'
            f'aws s3 cp s3://{plugins_bucket.bucket_name}/{zipname} /tmp/{zipname} --region {self.region}\n'
            f'unzip -q -o /tmp/{zipname} -d /opt/kafka-connect-plugins/{zipname[:-4]}\n'
            for zipname in plugin_zips
        )

        bc_fips_lines = ""
        if enable_phase2:
            bc_fips_lines = "\n".join(
                f"curl -sL -o /opt/kafka/libs/{artifact}-{version}.jar "
                f"https://repo1.maven.org/maven2/org/bouncycastle/{artifact}/{version}/{artifact}-{version}.jar"
                for artifact, version in BC_FIPS_JARS
            )

        # Topics must exist before connectors start: without this, a
        # producer/consumer's first metadata request for a still-missing
        # topic can leave the client hanging past its own request timeout
        # instead of cleanly auto-creating it (confirmed at runtime for both
        # the bridge Lambda's `iot-events` producer and Debezium's
        # `cdc.iot.iot_events` output topic).
        topics_to_create = ["iot-events"]
        if enable_phase2:
            topics_to_create.append("cdc.iot.iot_events")
        topic_create_lines = "\n".join(
            f'/opt/kafka/bin/kafka-topics.sh --bootstrap-server {bootstrap_brokers_iam} '
            f'--command-config /tmp/client.properties --create --if-not-exists '
            f'--topic {topic} --partitions 3 --replication-factor 2'
            for topic in topics_to_create
        )

        submit_lines = []
        for name, (config, secret_kind) in connector_files.items():
            config_json = json.dumps(config)
            if secret_kind == "postgres":
                submit_lines.append(
                    f"""cat > /opt/kafka-connect-configs/{name}.json <<'JSON_EOF'
{config_json}
JSON_EOF
jq --arg u "$PG_USER" --arg p "$PG_PASS" '.["connection.user"]=$u | .["connection.password"]=$p | .["database.user"]=$u | .["database.password"]=$p' /opt/kafka-connect-configs/{name}.json > /tmp/{name}.json && mv /tmp/{name}.json /opt/kafka-connect-configs/{name}.json
"""
                )
            elif secret_kind == "snowflake":
                submit_lines.append(
                    f"""cat > /opt/kafka-connect-configs/{name}.json <<'JSON_EOF'
{config_json}
JSON_EOF
jq --arg k "$SNOWFLAKE_PRIVATE_KEY" '.["snowflake.private.key"]=$k' /opt/kafka-connect-configs/{name}.json > /tmp/{name}.json && mv /tmp/{name}.json /opt/kafka-connect-configs/{name}.json
"""
                )
            else:
                submit_lines.append(
                    f"""cat > /opt/kafka-connect-configs/{name}.json <<'JSON_EOF'
{config_json}
JSON_EOF
"""
                )
            submit_lines.append(
                f'curl -sf -X PUT -H "Content-Type: application/json" '
                f'--data @/opt/kafka-connect-configs/{name}.json '
                f'http://localhost:8083/connectors/{name}/config\n'
            )
        submit_script = "\n".join(submit_lines)

        secret_fetch_lines = f"""PG_SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id {postgres_secret.secret_arn} --region {self.region} --query SecretString --output text)
PG_USER=$(echo "$PG_SECRET_JSON" | jq -r .username)
PG_PASS=$(echo "$PG_SECRET_JSON" | jq -r .password)
"""
        if enable_phase2:
            secret_fetch_lines += f"""SNOWFLAKE_SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id {snowflake_secret.secret_arn} --region {self.region} --query SecretString --output text)
SNOWFLAKE_PRIVATE_KEY=$(echo "$SNOWFLAKE_SECRET_JSON" | jq -r .private_key)
"""

        user_data_script = f"""#!/bin/bash
set -eux
dnf install -y java-17-amazon-corretto-headless jq unzip

mkdir -p /opt/kafka-connect-plugins /opt/kafka-connect-configs
curl -sL -o /tmp/{KAFKA_TGZ} {KAFKA_URL}
tar -xzf /tmp/{KAFKA_TGZ} -C /opt
ln -sfn /opt/kafka_2.13-{KAFKA_VERSION} /opt/kafka
curl -sL -o /opt/kafka/libs/aws-msk-iam-auth-{MSK_IAM_AUTH_VERSION}-all.jar {MSK_IAM_AUTH_URL}
{bc_fips_lines}

{plugin_fetch_lines}
cat > /opt/kafka/config/connect-distributed.properties <<'PROPS_EOF'
{connect_properties}
PROPS_EOF

cat > /tmp/client.properties <<'CLIENT_PROPS_EOF'
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
CLIENT_PROPS_EOF
{topic_create_lines}

cat > /etc/systemd/system/kafka-connect.service <<'UNIT_EOF'
[Unit]
Description=Kafka Connect (distributed worker) - iot-hack
After=network.target

[Service]
Type=simple
Environment=KAFKA_HEAP_OPTS=-Xms512m -Xmx1536m
ExecStart=/opt/kafka/bin/connect-distributed.sh /opt/kafka/config/connect-distributed.properties
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT_EOF

systemctl daemon-reload
systemctl enable --now kafka-connect

{secret_fetch_lines}
until curl -sf http://localhost:8083/connectors >/dev/null 2>&1; do sleep 5; done

{submit_script}
"""

        self.instance = ec2.Instance(
            self,
            "KafkaConnectEc2",
            vpc=vpc,
            vpc_subnets=public_subnet,
            associate_public_ip_address=True,
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            security_group=sg_kafka_connect,
            role=role,
            user_data=ec2.UserData.custom(user_data_script),
            block_devices=[
                ec2.BlockDevice(device_name="/dev/xvda", volume=ec2.BlockDeviceVolume.ebs(20, encrypted=True))
            ],
        )

        CfnOutput(self, "KafkaConnectInstanceId", value=self.instance.instance_id)
        CfnOutput(self, "KafkaConnectRestUrl", value="http://localhost:8083 (via SSM port-forward or SSM exec)")
