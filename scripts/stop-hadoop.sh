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
