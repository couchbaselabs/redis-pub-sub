# redis-pubsub

pub sub pipeline


docker run -d --name redis-stack --network hadoop-couchbase-network -p 6379:6379 -p 8001:8001 redis/redis-stack:latest

redis-cli
CONFIG SET notify-keyspace-events KEA

docker run --rm --name publisher -e REDIS_MODE=publish --network hadoop-couchbase-network redis-pubsub-app

docker run --rm -e REDIS_MODE=publish -e DOC_COUNT=1000 --network hadoop-couchbase-network redis-pubsub-app

docker run --rm --name subscribe -e REDIS_MODE=subscribe --network hadoop-couchbase-network redis-pubsub-app

