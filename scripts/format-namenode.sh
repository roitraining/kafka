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
rm -r /hdfs
mkdir /hdfs
mkdir /hdfs/data
mkdir /hdfs/nn
mkdir /hdfs/snn
hdfs namenode -format
echo "starting namenode"
/usr/local/hadoop/bin/hdfs --daemon start namenode
echo "starting secondary namenode"
/usr/local/hadoop/bin/hdfs --daemon start secondarynamenode
echo "starting datanode"
/usr/local/hadoop/bin/hdfs --daemon start datanode
echo "starting resource manager"
/usr/local/hadoop/bin/yarn --daemon start resourcemanager
echo "starting node manager"
/usr/local/hadoop/bin/yarn --daemon start nodemanager
jps
hadoop fs -mkdir /user
hadoop fs -mkdir /user/student
hadoop fs -mkdir /user/root
hadoop fs -mkdir /user/hive
hadoop fs -mkdir /user/hive/warehouse
hadoop fs -chown student:student /user/student

mysql -ppassword -e "drop database metastore;"
mysql -ppassword -e "create database metastore;"
mysql -ppassword -e "grant all privileges on *.* to 'test'@'localhost' identified by 'password';"
schematool -initSchema -dbType mysql

echo "starting hive service"
nohup hive --service metastore > /dev/null 2>&1 &
