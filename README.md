# redis-pubsub

pub sub pipeline


## create redis container
docker run -d --name redis-stack --network hadoop-couchbase-network -p 6379:6379 -p 8001:8001 redis/redis-stack:latest

## access the redis cli
redis-cli

## configure the event keyspace
CONFIG SET notify-keyspace-events KEA

## build the redis-pub-sub image
docker build -t redis-pubsub-app .

## this will start the subscriber
docker run --rm --name subscribe -e REDIS_MODE=subscribe --network hadoop-couchbase-network redis-pubsub-app

## this will create a container that publishes 1000 documents to redis
docker run --rm -e REDIS_MODE=publish -e DOC_COUNT=1000 --network hadoop-couchbase-network redis-pubsub-app



