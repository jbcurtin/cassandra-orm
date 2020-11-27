import os

ETL_CLUSTER_URIS = [uri.strip() for uri in os.environ.get('ETL_CLUSTER_URIS', '').split(',') if uri]
POSTGRESQL_URI = os.environ.get('POSTGRESQL_URI', None)
CASSANDRA_CONTAINER_NAME = os.environ.get('CASSANDRA_CONTAINER_NAME', None)
POSTRGESQL_CONTAINER_NAME = os.environ.get('POSTGRESQL_CONTAINER_NAME', None)
CORM_EXPORT_DIR = '/tmp/corm-exports'
