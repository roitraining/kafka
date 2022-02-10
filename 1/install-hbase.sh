#! /bin/sh
cd /tmp
wget https://dlcdn.apache.org/hbase/2.4.9/hbase-2.4.9-bin.tar.gz
wget https://dlcdn.apache.org/hbase/2.4.9/hbase-2.4.9-client-bin.tar.gz
tar xvzf hbase-2.4.9-bin.tar.gz -C /usr/local
ln -s /usr/local/hbase-2.4.9/ /usr/local/hbase
chown -h student:student /usr/local/hbase
chown student:student /usr/local/hbase-2.4.9/
