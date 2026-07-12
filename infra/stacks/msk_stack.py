from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_msk as msk
from aws_cdk import custom_resources as cr
from aws_cdk import aws_iam as iam
from constructs import Construct


class MskStack(Stack):
    """Provisioned MSK cluster - 2x kafka.t3.small brokers, IAM auth + TLS.

    t3.small is the smallest supported broker size: plenty for a hackathon's
    handful of simulated devices, and dramatically cheaper than m5.large for
    a same-day build. IAM client auth means no Kafka username/password ever
    exists to leak - producers/consumers authenticate via their IAM role.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        sg_msk: ec2.SecurityGroup,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        private_subnets = vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED).subnet_ids

        self.cluster = msk.CfnCluster(
            self,
            "IotEventsCluster",
            cluster_name="iot-hack-msk",
            kafka_version="3.6.0",
            number_of_broker_nodes=2,  # one broker per AZ (2 private-isolated subnets)
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.t3.small",
                client_subnets=private_subnets,
                security_groups=[sg_msk.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(volume_size=20)
                ),
            ),
            # IAM auth only. `tls=` here means *mutual TLS client-cert auth*
            # (requires an ACM Private CA list) - a different thing from the
            # TLS-in-transit wire encryption below, which we do want.
            client_authentication=msk.CfnCluster.ClientAuthenticationProperty(
                sasl=msk.CfnCluster.SaslProperty(
                    iam=msk.CfnCluster.IamProperty(enabled=True),
                ),
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS",
                    in_cluster=True,
                )
            ),
            enhanced_monitoring="DEFAULT",
        )

        # AWS::MSK::Cluster doesn't expose bootstrap-broker strings as a
        # CloudFormation Fn::GetAtt attribute at all (confirmed by deploy
        # error: "Requested attribute ... does not exist in schema") - they're
        # only available via the MSK GetBootstrapBrokers API. A custom
        # resource calls that API at deploy time so downstream stacks can
        # still consume the result as a normal cross-stack token.
        get_brokers = cr.AwsCustomResource(
            self,
            "GetBootstrapBrokers",
            on_create=cr.AwsSdkCall(
                service="Kafka",
                action="getBootstrapBrokers",
                parameters={"ClusterArn": self.cluster.attr_arn},
                physical_resource_id=cr.PhysicalResourceId.of("GetBootstrapBrokers"),
            ),
            on_update=cr.AwsSdkCall(
                service="Kafka",
                action="getBootstrapBrokers",
                parameters={"ClusterArn": self.cluster.attr_arn},
                physical_resource_id=cr.PhysicalResourceId.of("GetBootstrapBrokers"),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements(
                [iam.PolicyStatement(actions=["kafka:GetBootstrapBrokers"], resources=[self.cluster.attr_arn])]
            ),
        )
        get_brokers.node.add_dependency(self.cluster)

        self.bootstrap_brokers_iam = get_brokers.get_response_field("BootstrapBrokerStringSaslIam")

        CfnOutput(self, "MskClusterArn", value=self.cluster.attr_arn)
        CfnOutput(
            self,
            "MskBootstrapBrokersIam",
            value=self.bootstrap_brokers_iam,
            description="Bootstrap brokers for IAM-auth clients (bridge Lambda, Kafka Connect)",
        )
