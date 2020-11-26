from cassandra.auth import PlainTextAuthProvider
from corm.constants import CLUSTER_USERNAME, CLUSTER_PASSWORD

AuthProvider = None

# https://docs.datastax.com/en/developer/python-driver/3.24/api/cassandra/auth/#cassandra.auth.PlainTextAuthProvider
plain_auth = PlainTextAuthProvider(username=CLUSTER_USERNAME, password=CLUSTER_PASSWORD)

if CLUSTER_USERNAME:
    AuthProvider = plain_auth
