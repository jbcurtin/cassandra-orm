import os
import typing

ENCODING = 'utf-8'
CLUSTER_IPS = [cluster_ip for cluster_ip in os.environ['CLUSTER_IPS'].split(',') if cluster_ip]
if len(CLUSTER_IPS) < 1:
    raise NotImplementedError('CLUSTER_IPS ENVVar required')
CLUSTER_PORT = int(os.environ.get('CLUSTER_PORT', 9042))
CLUSTER_USERNAME = os.environ.get('CLUSTER_USERNAME', None)
CLUSTER_PASSWORD = os.environ.get('CLUSTER_PASSWORD', None)
TABLES = {}
SESSIONS = {}
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
PWN = typing.TypeVar('PWN')
