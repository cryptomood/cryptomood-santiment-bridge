# Cryptomood-Santiment bridge

## How to run
1. prepare your cert.pem file and place it ie. to the project root directory
2. `docker-compose up -d --build` will start 3 services - zookeeper, kafka & this bridge.

## How it works
Bridge will first use the Exporter module to connect to zookeeper instance.
Then it will connect to dedicated cryptomood api server.

Check the `docker-compose.yml` file for env variables. 

| name | example value | desctiption | 
|---|---|---|
| CRYPTOMOOD_URL | aaa.bbb.ccc:1234 | url that has been provided for you |
| CERT_FILE_PATH | ./cert.pem | path to .pem file |
| PROTO_FILE_PATH | ./types.proto | path to definitions |
| ONLY_HISTORIC | 0 | 1 = will only process historic data, 0 = same as 1, but will also subscribe for new data |
| CANDLE_TYPE | social | social or news |
| RESET_POSITION | 1564617600 | resets the last position of exporter to specific unix timestamp |