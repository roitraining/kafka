#! /bin/sh
mysql -ppassword -e "drop database metastore;"
mysql -ppassword -e "create database metastore;"
mysql -ppassword -e "grant all privileges on *.* to 'test'@'localhost' identified by 'password';"
schematool -initSchema -dbType mysql
hadoop fs -rm -r /regions
hadoop fs -rm -r /territories
hadoop fs -rm -r /user/hive/warehouse
hadoop fs -mkdir /user/hive/warehouse
nohup hive --service metastore > /dev/null 2>&1 &

#cat /class/regions.hql
#hive -i /class/regions.hql




