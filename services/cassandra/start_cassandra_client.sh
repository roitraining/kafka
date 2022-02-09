docker run -it --rm \
    --network cassandra_network \
    bitnami/cassandra:latest cqlsh --username cassandra --password  student cassandra

