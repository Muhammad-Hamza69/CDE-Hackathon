from aws_cdk import CfnOutput, Duration, Stack

from stacks._msk_arn import msk_resource_arn_wildcard
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_iot as iot
from aws_cdk import aws_lambda as _lambda
from constructs import Construct

DEVICE_COUNT = 5
MQTT_TOPIC_FILTER = "iot/+/geolocation"


class IotBridgeStack(Stack):
    """Task 1.2 + the IoT-Core-to-Kafka bridge.

    AWS IoT Core has no native "publish to MSK" rule action, so a Lambda
    bridges the two: an IoT Topic Rule invokes it with the MQTT payload as
    the event, and it republishes onto the MSK `iot-events` topic using IAM
    auth. Also provisions the IoT Things + a shared device policy for the
    5+ simulated devices in simulator/device_simulator.py.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        sg_bridge_lambda: ec2.SecurityGroup,
        bootstrap_brokers_iam: str,
        msk_cluster_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Task 1.2: IoT Things + shared device policy ---
        self.things = [
            iot.CfnThing(self, f"Device{i:03d}", thing_name=f"device-{i:03d}")
            for i in range(1, DEVICE_COUNT + 1)
        ]

        self.device_policy = iot.CfnPolicy(
            self,
            "DevicePolicy",
            policy_name="iot-hack-device-policy",
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "iot:Connect",
                        "Resource": f"arn:aws:iot:{self.region}:{self.account}:client/${{iot:Connection.Thing.ThingName}}",
                    },
                    {
                        "Effect": "Allow",
                        "Action": "iot:Publish",
                        "Resource": f"arn:aws:iot:{self.region}:{self.account}:topic/iot/*/geolocation",
                    },
                ],
            },
        )

        # --- Bridge Lambda: IoT Core Rule -> MSK (iot-events, IAM auth) ---
        bridge_role = iam.Role(
            self,
            "BridgeLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
            ],
        )
        topic_arn = msk_resource_arn_wildcard(msk_cluster_arn, "topic")
        bridge_role.add_to_policy(
            iam.PolicyStatement(
                actions=["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"],
                resources=[msk_cluster_arn],
            )
        )
        bridge_role.add_to_policy(
            iam.PolicyStatement(
                # CreateTopic is required even though the JDBC/S3 sink
                # connectors are the "real" owners of iot-events: the very
                # first producer request for a not-yet-existing topic
                # triggers auto-topic-creation (MSK's default
                # auto.create.topics.enable=true), and without CreateTopic
                # that metadata request just hangs until the Lambda times
                # out (confirmed - 30s timeout, no further log lines after
                # "Connection complete").
                actions=["kafka-cluster:WriteData", "kafka-cluster:DescribeTopic", "kafka-cluster:CreateTopic"],
                resources=[topic_arn],
            )
        )

        self.bridge_function = _lambda.Function(
            self,
            "IotKafkaBridge",
            # kafka-python 2.0.2's vendored six.py fakes the `six.moves`
            # submodule via the legacy find_module/load_module import-hook
            # API, which Python 3.12 removed entirely (only find_spec is
            # called now) - confirmed by the Lambda's actual runtime error:
            # "No module named 'kafka.vendor.six.moves'". 3.11 still
            # supports the old hook, so the vendored six.py works there.
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=_lambda.Code.from_asset("../iot-bridge-lambda"),
            role=bridge_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[sg_bridge_lambda],
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "MSK_BOOTSTRAP_BROKERS": bootstrap_brokers_iam,
                "KAFKA_TOPIC": "iot-events",
            },
        )

        # --- Task 1.2: IoT Core Rule routing MQTT -> Lambda ---
        self.topic_rule = iot.CfnTopicRule(
            self,
            "GeoLocationToKafkaRule",
            rule_name="iot_geolocation_to_kafka",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql=f"SELECT * FROM '{MQTT_TOPIC_FILTER}'",
                aws_iot_sql_version="2016-03-23",
                description="Routes simulated geoLocation MQTT messages to the IoT->Kafka bridge Lambda",
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(function_arn=self.bridge_function.function_arn)
                    )
                ],
            ),
        )

        self.bridge_function.add_permission(
            "AllowIotInvoke",
            principal=iam.ServicePrincipal("iot.amazonaws.com"),
            source_arn=self.topic_rule.attr_arn,
        )

        CfnOutput(self, "DevicePolicyName", value=self.device_policy.policy_name)
        CfnOutput(self, "BridgeLambdaName", value=self.bridge_function.function_name)
        CfnOutput(self, "BridgeLambdaLogGroup", value=f"/aws/lambda/{self.bridge_function.function_name}")
