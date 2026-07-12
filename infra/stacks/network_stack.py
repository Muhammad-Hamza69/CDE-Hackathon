from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class NetworkStack(Stack):
    """VPC for the IoT hackathon pipeline.

    Layout (2 AZs, no NAT Gateway — cost-conscious for a same-day hackathon):
      - PUBLIC subnets: Kafka Connect (MSK Connect) ENIs only. These need real
        internet egress to reach the Snowflake API in Phase 2, so they get an
        IGW route + a locked-down security group (no inbound, minimal outbound)
        instead of paying for a NAT Gateway.
      - PRIVATE_ISOLATED subnets: Bastion EC2, PostgreSQL EC2, MSK brokers.
        None of these have a route to the internet. SSM Session Manager access
        (bastion + Postgres) works anyway via VPC interface endpoints for
        ssm/ssmmessages/ec2messages, matching the PDF's "Bastion Host EC2 - no
        public access" + "SSM Session Manager access" requirement exactly.
        A free S3 gateway endpoint lets Amazon Linux 2023 pull packages
        (its package repos are S3-backed) without any internet route.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "IotHackVpc",
            ip_addresses=ec2.IpAddresses.cidr("10.42.0.0/16"),
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public-connect",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="private-isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # Free gateway endpoint: lets AL2023 dnf + Kafka Connect S3 plugin/backup
        # access work from subnets with no internet route.
        self.vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
            subnets=[ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED)],
        )

        # Interface endpoints required for SSM Session Manager to reach fully
        # private (no NAT/no IGW) instances, plus Secrets Manager so Postgres
        # user-data can fetch its master password without internet.
        interface_services = {
            "SsmEndpoint": ec2.InterfaceVpcEndpointAwsService.SSM,
            "SsmMessagesEndpoint": ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES,
            "Ec2MessagesEndpoint": ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES,
            "SecretsManagerEndpoint": ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            # Used by the IoT->Kafka bridge Lambda's MSK IAM-auth token signer.
            "StsEndpoint": ec2.InterfaceVpcEndpointAwsService.STS,
        }
        self.sg_vpc_endpoints = ec2.SecurityGroup(
            self,
            "VpcEndpointsSg",
            vpc=self.vpc,
            description="Allow HTTPS from private-subnet resources to VPC interface endpoints",
            allow_all_outbound=False,
        )
        for endpoint_id, service in interface_services.items():
            self.vpc.add_interface_endpoint(
                endpoint_id,
                service=service,
                subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
                security_groups=[self.sg_vpc_endpoints],
                private_dns_enabled=True,
            )

        # --- Security groups (locked down, rules wired up by consuming stacks) ---
        self.sg_bastion = ec2.SecurityGroup(
            self,
            "BastionSg",
            vpc=self.vpc,
            description="Bastion EC2 - SSM only, no inbound rules at all",
            allow_all_outbound=False,
        )

        self.sg_postgres = ec2.SecurityGroup(
            self,
            "PostgresSg",
            vpc=self.vpc,
            description="PostgreSQL EC2 - inbound 5432 from Kafka Connect + Bastion only",
            allow_all_outbound=False,
        )

        self.sg_msk = ec2.SecurityGroup(
            self,
            "MskSg",
            vpc=self.vpc,
            description="MSK brokers - inbound Kafka ports from Kafka Connect + bridge Lambda only",
            allow_all_outbound=True,  # brokers reply to clients on ephemeral ports
        )

        self.sg_kafka_connect = ec2.SecurityGroup(
            self,
            "KafkaConnectSg",
            vpc=self.vpc,
            description="MSK Connect ENIs (public subnet) - no inbound, outbound 443/9098 only",
            allow_all_outbound=False,
        )
        self.sg_kafka_connect.add_egress_rule(
            ec2.Peer.any_ipv4(), ec2.Port.tcp(443), "HTTPS: Snowflake API, AWS APIs"
        )
        # Snowflake's JDBC driver does OCSP (certificate revocation) checks
        # against ocsp.snowflakecomputing.com/ocsp.digicert.com over plain
        # HTTP - without this, those checks fail with "Network is
        # unreachable" and retry with growing backoff for minutes before the
        # Snowflake sink connector's own validation request times out
        # (confirmed at runtime).
        self.sg_kafka_connect.add_egress_rule(
            ec2.Peer.any_ipv4(), ec2.Port.tcp(80), "HTTP: Snowflake JDBC driver OCSP checks"
        )

        self.sg_bridge_lambda = ec2.SecurityGroup(
            self,
            "BridgeLambdaSg",
            vpc=self.vpc,
            description="IoT to Kafka bridge Lambda (VPC-attached to reach MSK)",
            allow_all_outbound=True,
        )

        # Postgres: 5432 from Kafka Connect (JDBC sink + Debezium source) and Bastion
        self.sg_postgres.add_ingress_rule(self.sg_kafka_connect, ec2.Port.tcp(5432), "Kafka Connect JDBC/Debezium")
        self.sg_postgres.add_ingress_rule(self.sg_bastion, ec2.Port.tcp(5432), "Bastion admin access")
        self.sg_postgres.add_egress_rule(self.sg_vpc_endpoints, ec2.Port.tcp(443), "Secrets Manager / SSM endpoints")
        self.sg_postgres.add_egress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(443), "S3 gateway endpoint (package repos)")

        self.sg_bastion.add_egress_rule(self.sg_vpc_endpoints, ec2.Port.tcp(443), "SSM endpoints")
        self.sg_bastion.add_egress_rule(self.sg_postgres, ec2.Port.tcp(5432), "psql to Postgres")
        self.sg_bastion.add_egress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(443), "S3 gateway endpoint (package repos)")

        # MSK: IAM-auth (9098) + TLS (9094) from Kafka Connect and the bridge Lambda
        for sg_client in (self.sg_kafka_connect, self.sg_bridge_lambda):
            self.sg_msk.add_ingress_rule(sg_client, ec2.Port.tcp(9098), "Kafka IAM auth")
            self.sg_msk.add_ingress_rule(sg_client, ec2.Port.tcp(9094), "Kafka TLS")

        self.sg_kafka_connect.add_egress_rule(self.sg_msk, ec2.Port.tcp(9098), "Produce/consume MSK (IAM)")
        self.sg_kafka_connect.add_egress_rule(self.sg_msk, ec2.Port.tcp(9094), "Produce/consume MSK (TLS)")
        self.sg_kafka_connect.add_egress_rule(self.sg_postgres, ec2.Port.tcp(5432), "JDBC sink / Debezium source")

        # VPC interface endpoints (SSM/SecretsManager/STS): allow inbound 443
        # from every private-subnet client that needs them.
        for sg_client, desc in (
            (self.sg_postgres, "Postgres to Secrets Manager/SSM"),
            (self.sg_bastion, "Bastion to SSM"),
            (self.sg_bridge_lambda, "Bridge Lambda to STS"),
        ):
            self.sg_vpc_endpoints.add_ingress_rule(sg_client, ec2.Port.tcp(443), desc)
