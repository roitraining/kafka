#! /bin/bash
# make sure the hbase service is running
# start-hbase.sh
cd blogposts
sh create-time-stamp-blogposts.bash
sh populate-blogposts.bash
sh start-thrift.sh
python test-blogposts.py
