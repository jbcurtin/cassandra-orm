import os

from corm.etl.datatypes import ConnectionInfo

POSTGRESQL_URI = os.environ.get('POSTGRESQL_URI', None)
POSTGRESQL_INFO = ConnectionInfo.From_URI(POSTGRESQL_URI)
CASSANDRA_CONTAINER_NAME = os.environ.get('CASSANDRA_CONTAINER_NAME', None)
POSTRGESQL_CONTAINER_NAME = os.environ.get('POSTGRESQL_CONTAINER_NAME', None)
