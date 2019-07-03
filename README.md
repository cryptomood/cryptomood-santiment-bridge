# Cryptomood-Santiment bridge

## How to run
1. prepare your cert.pem file and place it ie. to the project root directory
2. `docker-compose up -d --build` will start 3 services - zookeeper, kafka & this bridge.

## How it works
Bridge will first use the Exporter module to connect to zookeeper instance.
Then it will connect to dedicated cryptomood api server.

Check the `docker-compose.yml` file for env variables. 