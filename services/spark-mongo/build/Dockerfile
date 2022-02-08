FROM bitnami/spark
USER root
RUN pip install pymongo
RUN pip install pyspark
RUN curl --location \
    https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/2.4.3/mongo-spark-connector_2.12-2.4.3.jar \
    --output /opt/bitnami/spark/jars/mongo-spark-connector_2.12-2.4.3.jar
RUN curl --location \    
    https://search.maven.org/remotecontent?filepath=org/mongodb/bson/4.4.1/bson-4.4.1.jar \
    --output /opt/bitnami/spark/jars/bson-4.4.1.jar
RUN curl --location \    
    https://search.maven.org/remotecontent?filepath=org/mongodb/mongodb-driver-core/4.4.1/mongodb-driver-core-4.4.1.jar \
    --output /opt/bitnami/spark/jars/mongodb-driver-core-4.4.1.jar
RUN curl --location \    
    https://search.maven.org/remotecontent?filepath=org/mongodb/mongo-java-driver/3.12.10/mongo-java-driver-3.12.10.jar \
    --output /opt/bitnami/spark/jars/mongo-java-driver-3.12.10.jar
USER 1001
