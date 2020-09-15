import os
import typing

ENCODING = 'utf-8'
CLUSTER_IPS = [cluster_ip for cluster_ip in os.environ['CLUSTER_IPS'].split(',') if cluster_ip]
CLUSTER_PORT = int(os.environ.get('CLUSTER_PORT', 9042))
TABLES = {}
SESSIONS = {}
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
PWN = typing.TypeVar('PWN')
