from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
ap = PlainTextAuthProvider(username='cassandra', password='student')
c = Cluster(['localhost'], auth_provider=ap)
c.connect()
