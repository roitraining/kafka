# pip install cassandra-driver
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 1-spark-cassandra-test.py

import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

#CASSANDRA_HOST = 'localhost'
CASSANDRA_HOST = '127.0.0.1'
CASSANDRA_USER = 'cassandra'
CASSANDRA_PASSWORD = 'student'

ap = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
cluster = Cluster([CASSANDRA_HOST], auth_provider = ap)
cluster.connect()

import sys
sys.path.append('/class')
from initspark import initspark
sc, spark, conf = initspark(cassandra = "127.0.0.1", cassandra_user = 'cassandra', cassandra_password='student')

session = cluster.connect()
session.execute('DROP KEYSPACE IF EXISTS classroom')
session.execute("CREATE KEYSPACE classroom WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':'1'}")
session = cluster.connect('classroom')
session.execute("create table student(id int PRIMARY KEY, firstname text, lastname text, emails set<text>)")
session.execute("insert into student (id, firstname, lastname, emails) values (1, 'Joe', 'Smith', {'joes@xyz.com', 'joe.smith@abc.net'})")
session.execute("update student set firstname = 'Joseph' where id = 1")
session.execute("insert into student (id, firstname, lastname, emails) values (2, 'Mike', 'Jones', {'mikej@xyz.com', 'mike.jones@def.net', 'mike1234@gmail.com'})")
rows = session.execute('SELECT id, firstname, lastname, emails from student')
print('*' * 80)
print('student rows from cassandra directly')
print('*' * 80)
print(list(rows))
print('*' * 80)

# Python to access a Cassandra cluster through Spark
people = spark.read.format("org.apache.spark.sql.cassandra").options(table="student", keyspace="classroom").load()
print('*' * 80)
print('student rows from spark before insert')
print('*' * 80)
people.show()
print(people.collect())
print('*' * 80)

# Append the results of a DataFrame into a Cassandra table
x = sc.parallelize([(3, 'Mary', 'Johnson', ['Mary1@gmail.com', 'Mary2@yahoo.com'])])
x1 = spark.createDataFrame(x, schema = ['id', 'firstname', 'lastname', 'emails'])
x1.write.format("org.apache.spark.sql.cassandra").options(table="student", keyspace="classroom").mode("append").save()

people = spark.read.format("org.apache.spark.sql.cassandra").options(table="student", keyspace="classroom").load()
print('*' * 80)
print('student rows from spark after insert')
print('*' * 80)
people.show()
print(people.collect())
print('*' * 80)

print('*' * 80)
print('spark sql query from cassandra')
print('*' * 80)
people.createOrReplaceTempView('people')
people2 = spark.sql('select id, firstname, lastname, email from people LATERAL VIEW EXPLODE(emails) EXPLODED_TABLE AS email')
people2.show()

people3 = people2.where("email like '%.com'").orderBy("id")
people3.show()

