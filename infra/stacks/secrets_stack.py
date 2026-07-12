from aws_cdk import RemovalPolicy, SecretValue, Stack
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct


class SecretsStack(Stack):
    """Centralizes credentials so nothing sensitive ever lands in code or the repo.

    - postgres_secret: auto-generated master password for the on-prem-sim
      PostgreSQL EC2 instance. Read at boot by Postgres user-data and by every
      Kafka Connect connector that talks to Postgres (JDBC sink, Debezium
      source) via Connect's Secrets Manager config provider.
    - snowflake_secret: placeholder populated manually AFTER `cdk deploy`
      (Phase 2) with the Snowflake Kafka Connector's RSA private key. CDK
      only creates the empty secret shell; the sensitive value is never in
      source control or CDK code.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.postgres_secret = secretsmanager.Secret(
            self,
            "PostgresMasterSecret",
            description="Master credentials for the on-prem-simulation PostgreSQL EC2 instance",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"iot_admin"}',
                generate_string_key="password",
                exclude_punctuation=True,
                password_length=24,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Empty shell — fill in manually post-deploy via:
        #   aws secretsmanager put-secret-value --secret-id <arn> \
        #     --secret-string '{"private_key":"...","private_key_passphrase":"..."}'
        self.snowflake_secret = secretsmanager.Secret(
            self,
            "SnowflakeKeypairSecret",
            description="Snowflake Kafka Connector RSA key-pair auth (populated manually after Snowflake setup)",
            secret_object_value={
                "private_key": SecretValue.unsafe_plain_text("REPLACE_ME_AFTER_SNOWFLAKE_SETUP"),
            },
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Task 2.5 (bonus): password-auth read-only creds for the Timestream
        # writer Lambda (plain SELECT over CLEAN.IOT_VALIDATED - key-pair auth
        # is only wired up for the Kafka Connector service user above).
        # Empty shell — fill in manually post-deploy via:
        #   aws secretsmanager put-secret-value --secret-id <arn> \
        #     --secret-string '{"account":"...","user":"...","password":"...","role":"...","warehouse":"..."}'
        self.snowflake_reader_secret = secretsmanager.Secret(
            self,
            "SnowflakeReaderSecret",
            description="Snowflake read-only credentials for the Timestream writer Lambda (Task 2.5 bonus)",
            secret_object_value={
                "account": SecretValue.unsafe_plain_text("REPLACE_ME_AFTER_SNOWFLAKE_SETUP"),
                "user": SecretValue.unsafe_plain_text("REPLACE_ME_AFTER_SNOWFLAKE_SETUP"),
                "password": SecretValue.unsafe_plain_text("REPLACE_ME_AFTER_SNOWFLAKE_SETUP"),
                "role": SecretValue.unsafe_plain_text("REPLACE_ME_AFTER_SNOWFLAKE_SETUP"),
                "warehouse": SecretValue.unsafe_plain_text("REPLACE_ME_AFTER_SNOWFLAKE_SETUP"),
            },
            removal_policy=RemovalPolicy.DESTROY,
        )
