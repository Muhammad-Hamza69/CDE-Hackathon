from pathlib import Path

from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

SQL_DIR = Path(__file__).resolve().parents[2] / "postgres"


class PostgresStack(Stack):
    """PostgreSQL EC2 (on-prem simulation) + Bastion EC2, both fully private.

    Neither instance has a public IP or an internet route. Administrator
    access is exclusively via AWS SSM Session Manager (Task 1.4: "no direct
    SSH key required"), which works here through the VPC interface endpoints
    created in NetworkStack. Postgres pulls its master password from
    Secrets Manager at boot rather than having it baked into user-data.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        sg_postgres: ec2.SecurityGroup,
        sg_bastion: ec2.SecurityGroup,
        postgres_secret: secretsmanager.Secret,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        private_subnet_selection = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED)
        ami = ec2.MachineImage.latest_amazon_linux2023()

        # --- SSM-only instance roles (no SSH keys anywhere) ---
        postgres_role = iam.Role(
            self,
            "PostgresInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")],
        )
        postgres_secret.grant_read(postgres_role)

        bastion_role = iam.Role(
            self,
            "BastionInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")],
        )

        vpc_cidr = vpc.vpc_cidr_block
        init_sql = (SQL_DIR / "init.sql").read_text()

        postgres_script = f"""#!/bin/bash
set -eux
dnf install -y postgresql15 postgresql15-server jq
postgresql-setup --initdb
PGDATA=/var/lib/pgsql/data
sed -i "s/^#listen_addresses.*/listen_addresses = '*'/" $PGDATA/postgresql.conf
sed -i "s/^#wal_level.*/wal_level = logical/" $PGDATA/postgresql.conf
echo "max_wal_senders = 10" >> $PGDATA/postgresql.conf
echo "max_replication_slots = 10" >> $PGDATA/postgresql.conf
echo "host all all {vpc_cidr} scram-sha-256" >> $PGDATA/pg_hba.conf
echo "host replication all {vpc_cidr} scram-sha-256" >> $PGDATA/pg_hba.conf
systemctl enable --now postgresql

SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id {postgres_secret.secret_arn} --region {self.region} --query SecretString --output text)
PG_USER=$(echo "$SECRET_JSON" | jq -r .username)
PG_PASS=$(echo "$SECRET_JSON" | jq -r .password)
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD '$PG_PASS';"
sudo -u postgres psql -c "CREATE ROLE $PG_USER WITH LOGIN SUPERUSER PASSWORD '$PG_PASS';"

cat > /tmp/init.sql <<'PGSQL_EOF'
{init_sql}
PGSQL_EOF
sudo -u postgres psql -f /tmp/init.sql
systemctl restart postgresql
"""
        postgres_user_data = ec2.UserData.custom(postgres_script)

        self.postgres_instance = ec2.Instance(
            self,
            "PostgresEc2",
            vpc=vpc,
            vpc_subnets=private_subnet_selection,
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
            machine_image=ami,
            security_group=sg_postgres,
            role=postgres_role,
            user_data=postgres_user_data,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(20, encrypted=True),
                )
            ],
        )

        bastion_user_data = ec2.UserData.for_linux()
        bastion_user_data.add_commands(
            "set -eux",
            "dnf install -y postgresql15",
        )

        self.bastion_instance = ec2.Instance(
            self,
            "BastionEc2",
            vpc=vpc,
            vpc_subnets=private_subnet_selection,
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
            machine_image=ami,
            security_group=sg_bastion,
            role=bastion_role,
            user_data=bastion_user_data,
        )

        CfnOutput(self, "PostgresPrivateIp", value=self.postgres_instance.instance_private_ip)
        CfnOutput(self, "BastionInstanceId", value=self.bastion_instance.instance_id)
        CfnOutput(
            self,
            "SsmConnectBastion",
            value=f"aws ssm start-session --target {self.bastion_instance.instance_id}",
        )
