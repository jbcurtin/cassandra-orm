import os

CORM_EXPORT_DIR = '/tmp/corm-exports'
PSQL_CLUSTER_PORT = int(os.environ.get('PSQL_CLUSTER_PORT', 5432))
PSQL_URI = os.environ.get('PSQL_URI', None)
