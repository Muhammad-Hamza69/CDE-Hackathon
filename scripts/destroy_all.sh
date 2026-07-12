#!/usr/bin/env bash
# Tears down every stack (MSK/EC2 bill continuously - run this after grading/demo).
set -euo pipefail
cd "$(dirname "$0")/../infra"
export JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1
npx aws-cdk destroy --all --force
