Steps for installing standalone HBase
01) Navigate to:  https://hbase.apache.org/downloads.html 
02) Download version 2.4.9 bin and client-bin; note that 2.4.9 is 'stable'
03) Verify the integrity of the downloaded files: 
    a) sha512sum hbase-2.4.9-bin.tar.gz
    b) sha512sum hbase-2.4.9-client-bin.tar.gz
04) Extract the server archive to /usr/local:  
    a) sudo tar xvzf hbase-2.4.9-bin.tar.gz -C /usr/local
05) Execute: 
    a) cd /usr/local
    b) sudo ln -s hbase-2.4.9/ hbase
    c) sudo chown -h student:student hbase
    d) sudo chown student:student hbase-2.4.9/
06) Verify Java 1.8 is installed
07) Verify JAVA_HOME environment variable is set.
08) Add the following two environment variables to .bashrc
    a)  export HBASE_HOME=/usr/local/hbase
    b)  export PATH=$PATH:$HBASE_HOME/bin
09) Edit /usr/local/hbase/conf/hbase-site.xml
    a) change the value of hbase.tmp.dir from ./tmp to /tmp/hbase
    b) Zookeeper and HBase writhe to this directory
    b) In addition, hbase writes lots of logs to /usr/local/hbase/logs
10) Start Hadoop
    a) start-hadoop.sh 
11) Start HBase
    a) ./start-hbase.sh
12) Navigate to http://localhost:16010
