#!/usr/bin/env bash
# Adds the Debezium source + Snowflake sink connectors to Kafka Connect and
# runs dbt. Run scripts/setup_snowflake.py first (see README section 5).
# Requires SNOWFLAKE_ACCOUNT to be set (e.g. SNOWFLAKE_ACCOUNT=ORGNAME-ACCOUNTNAME ./deploy_phase2.sh).
set -euo pipefail
: "${SNOWFLAKE_ACCOUNT:?SNOWFLAKE_ACCOUNT must be set}"
cd "$(dirname "$0")/../infra"
export JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1
export ENABLE_PHASE2=true

npx aws-cdk deploy IotHackKafkaConnectStack --require-approval never

cd ../dbt/iot_analytics
dbt deps
dbt run --select staging silver gold
dbt test
echo "Phase 2 deployed. Next: streamlit run streamlit/app.py"
