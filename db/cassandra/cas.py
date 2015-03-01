from cassandra.cluster import Cluster

cluster = Cluster(['172.31.53.229'])

session = cluster.connect('example')

rows = session.execute('SELECT * from votes')
for user_row in rows:
        print user_row.id, user_row.value
