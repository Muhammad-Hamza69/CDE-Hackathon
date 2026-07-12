#!/usr/bin/env bash
# One-time setup: creates an X.509 cert per simulated device, attaches the
# shared IoT policy (created by IotHackIotBridgeStack) and the device's
# CfnThing, and downloads Amazon's root CA. Certs/keys land in
# simulator/certs/<device_id>/ - gitignored, never committed.
set -euo pipefail

DEVICE_COUNT="${1:-5}"
POLICY_NAME="iot-hack-device-policy"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS_DIR="$SCRIPT_DIR/certs"

mkdir -p "$CERTS_DIR"
if [ ! -f "$CERTS_DIR/AmazonRootCA1.pem" ]; then
  curl -s -o "$CERTS_DIR/AmazonRootCA1.pem" https://www.amazontrust.com/repository/AmazonRootCA1.pem
fi

for i in $(seq -w 1 "$DEVICE_COUNT"); do
  DEVICE_ID="device-$(printf '%03d' "$((10#$i))")"
  DEVICE_DIR="$CERTS_DIR/$DEVICE_ID"
  mkdir -p "$DEVICE_DIR"

  if [ -f "$DEVICE_DIR/cert.pem.crt" ]; then
    echo "[$DEVICE_ID] cert already provisioned, skipping"
    continue
  fi

  echo "[$DEVICE_ID] creating certificate..."
  RESULT=$(aws iot create-keys-and-certificate \
    --set-as-active \
    --certificate-pem-outfile "$DEVICE_DIR/cert.pem.crt" \
    --public-key-outfile "$DEVICE_DIR/public.pem.key" \
    --private-key-outfile "$DEVICE_DIR/private.pem.key")

  CERT_ARN=$(echo "$RESULT" | python -c "import json,sys; print(json.load(sys.stdin)['certificateArn'])")

  aws iot attach-policy --policy-name "$POLICY_NAME" --target "$CERT_ARN"
  aws iot attach-thing-principal --thing-name "$DEVICE_ID" --principal "$CERT_ARN"

  echo "[$DEVICE_ID] provisioned, cert attached to thing + policy"
done

echo "Done. Discover your IoT endpoint with:"
echo "  aws iot describe-endpoint --endpoint-type iot:Data-ATS --query endpointAddress --output text"
