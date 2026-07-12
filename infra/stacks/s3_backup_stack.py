from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from constructs import Construct


class S3BackupStack(Stack):
    """Backup bucket for the optional Kafka S3 Sink Connector (Task 1.3).

    Also doubles as the staging location for Kafka Connect custom plugin
    JARs (JDBC driver, Debezium, Snowflake connector) referenced by
    KafkaConnectStack.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.backup_bucket = s3.Bucket(
            self,
            "IotBackupBucket",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-old-backups",
                    expiration=Duration.days(14),
                    noncurrent_version_expiration=Duration.days(7),
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.plugins_bucket = s3.Bucket(
            self,
            "KafkaConnectPluginsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        CfnOutput(self, "BackupBucketName", value=self.backup_bucket.bucket_name)
        CfnOutput(self, "PluginsBucketName", value=self.plugins_bucket.bucket_name)
