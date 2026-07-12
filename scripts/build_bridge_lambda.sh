#!/usr/bin/env bash
# Vendors the bridge Lambda's third-party dependencies into iot-bridge-lambda/
# so CDK can zip the whole directory as a plain Code.from_asset with no
# Docker bundling required. Run this before `cdk deploy` whenever
# iot-bridge-lambda/requirements.txt changes.
set -euo pipefail
cd "$(dirname "$0")/../iot-bridge-lambda"
python -m pip install -q -r requirements.txt --target . --upgrade
echo "Vendored dependencies into iot-bridge-lambda/"
