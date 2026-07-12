from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

TABLE_NAME = "iot-hack-timeseries"


class DynamoDbStack(Stack):
    """Task 2.5 (bonus): DynamoDB time-series table + the Lambda that feeds
    it from Snowflake on an EventBridge schedule. Built last, only if time
    remains after Phase 1 + Phase 2 core are verified (per the build plan).

    Originally built against AWS Timestream per the brief, but Timestream for
    LiveAnalytics is closed to new AWS accounts ("Only existing Timestream
    for LiveAnalytics customers can access the service", HandlerErrorCode:
    GeneralServiceException - confirmed via a live `cdk deploy`, not a
    guess). DynamoDB's device_id (partition) + event_time epoch-millis
    (sort) key gives the same per-device time-range query pattern a
    dashboard needs, queryable from Grafana via a community DynamoDB data
    source plugin instead of Timestream's native one.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        snowflake_reader_secret: secretsmanager.Secret,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.table = dynamodb.Table(
            self,
            "TimeseriesTable",
            table_name=TABLE_NAME,
            partition_key=dynamodb.Attribute(name="device_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="event_time", type=dynamodb.AttributeType.NUMBER),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        role = iam.Role(self, "DynamoDbWriterRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )
        self.table.grant_read_write_data(role)
        snowflake_reader_secret.grant_read(role)

        self.writer_function = _lambda.Function(
            self,
            "DynamoDbWriter",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=_lambda.Code.from_asset("../lambda-dynamodb"),
            role=role,
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "DYNAMODB_TABLE": TABLE_NAME,
                "SNOWFLAKE_SECRET_ARN": snowflake_reader_secret.secret_arn,
            },
        )

        rule = events.Rule(self, "EveryMinute", schedule=events.Schedule.rate(Duration.minutes(1)))
        rule.add_target(targets.LambdaFunction(self.writer_function))

        CfnOutput(self, "DynamoDbTableName", value=TABLE_NAME)
