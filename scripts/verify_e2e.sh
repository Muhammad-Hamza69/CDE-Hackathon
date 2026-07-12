#!/usr/bin/env bash
# Smoke-test helpers for the Phase 1 -> Phase 2 demo (Task: "5-minute demo:
# show live CDC event flowing from PostgreSQL to Snowflake Gold").
set -euo pipefail

echo "== Kafka Connect connector status (self-managed EC2 - see architecture.md for why) =="
CONNECT_INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name IotHackKafkaConnectStack \
  --query "Stacks[0].Outputs[?OutputKey=='KafkaConnectInstanceId'].OutputValue" --output text)
CMD_ID=$(aws ssm send-command --instance-ids "$CONNECT_INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters 'commands=["for c in $(curl -s http://localhost:8083/connectors | jq -r .[]); do curl -s http://localhost:8083/connectors/$c/status | jq -c \"{name: .name, connector: .connector.state, tasks: [.tasks[].state]}\"; done"]' \
  --query "Command.CommandId" --output text)
sleep 8
aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$CONNECT_INSTANCE_ID" --query "StandardOutputContent" --output text

echo "== Bridge Lambda: last 5 minutes of logs =="
BRIDGE_FN=$(aws cloudformation describe-stacks --stack-name IotHackIotBridgeStack \
  --query "Stacks[0].Outputs[?OutputKey=='BridgeLambdaName'].OutputValue" --output text)
aws logs tail "/aws/lambda/$BRIDGE_FN" --since 5m --format short || true

echo "== Postgres row count (via SSM + psql on the bastion) =="
BASTION_ID=$(aws cloudformation describe-stacks --stack-name IotHackPostgresStack \
  --query "Stacks[0].Outputs[?OutputKey=='BastionInstanceId'].OutputValue" --output text)
echo "Run manually: aws ssm start-session --target $BASTION_ID"
echo "Then on the bastion: psql -h <PostgresPrivateIp> -U iot_admin -d postgres -c 'select count(*) from iot.iot_events;'"

echo "== S3 backup object count =="
BACKUP_BUCKET=$(aws cloudformation describe-stacks --stack-name IotHackS3BackupStack \
  --query "Stacks[0].Outputs[?OutputKey=='BackupBucketName'].OutputValue" --output text)
aws s3 ls "s3://$BACKUP_BUCKET" --recursive --summarize | tail -5
