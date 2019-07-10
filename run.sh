#!/bin/bash
cd "$(dirname "$0")"

export CRYPTOMOOD_URL=santiment.api.cryptomood.com:30002
export CERT_FILE_PATH="./cert.pem"
export PROTO_FILE_PATH="./types.proto"
export ONLY_HISTORIC=1
export CANDLE_TYPE=social
node ./index.js