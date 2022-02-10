#! /bin/sh
echo "stopping namenode"
/usr/local/hadoop/bin/hdfs --daemon stop namenode
echo "stopping secondary namenode"
/usr/local/hadoop/bin/hdfs --daemon stop secondarynamenode
echo "stopping datanode"
/usr/local/hadoop/bin/hdfs --daemon stop datanode
echo "stopping resource manager"
/usr/local/hadoop/bin/yarn --daemon stop resourcemanager
echo "stopping node manager"
/usr/local/hadoop/bin/yarn --daemon stop nodemanager

#docker rm -f bigdata
docker run --name bigdata --hostname bigdata \
-p 50070:50070 -p 8088:8088 -p 10020:10020 -p 9042:9042 -p 10000:10000 \
-p 10001:10001 -p 10002:10002 \
-v "$HOME/ROI/JPMC-Hadoop:/class" \
-v "$HOME:/host" -it joegagliardo/bigdata /etc/bootstrap.sh -bash

