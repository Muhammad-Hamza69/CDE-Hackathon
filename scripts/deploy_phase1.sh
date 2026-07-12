#!/usr/bin/env bash
# Deploys everything Phase 1 needs, in dependency order. Requires the Kafka
# Connect plugin JARs to already be staged (scripts/stage_kafka_connect_plugins.py).
set -euo pipefail
cd "$(dirname "$0")/../infra"
export JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1

npx aws-cdk deploy IotHackNetworkStack --require-approval never
npx aws-cdk deploy IotHackMskStack IotHackPostgresStack IotHackS3BackupStack \
  --require-approval never --concurrency 3
npx aws-cdk deploy IotHackKafkaConnectStack IotHackIotBridgeStack --require-approval never

echo "Phase 1 infrastructure deployed. Next: bash simulator/provision_things.sh && python simulator/device_simulator.py ..."
