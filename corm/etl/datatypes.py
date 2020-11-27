import enum
import typing

from corm.constants import PWN
from corm.etl.helpers import rationalize_docker_containers

from urllib.parse import urlparse

class DBEngine(enum.Enum):
    PostgreSQL = ['postgres', 'postgresql', 'psql']
    Cassandra = ['cassandra']

class ConnectionInfo(typing.NamedTuple):
    engine: DBEngine
    username: str
    password: str
    name: str
    port: str
    host: str
    @classmethod
    def From_URI(cls, uri: str) -> PWN:
        uri_parts = urlparse(uri)
        try:
            auth_info, net_info = uri_parts.netloc.split('@', 1)
        except ValueError:
            auth_info = None
            username, password = None, None
            net_info = uri_parts.netloc

        else: 
            username, password = auth_info.split(':', 1)
            username = username or None
            password = password or None

        host, port = net_info.split(':', 1)
        port = int(port)
        name = uri_parts.path.strip('/') or None
        for member in DBEngine.__members__.values():
            if uri_parts.scheme in member.value:
                engine = member
                break
        else:
            raise NotImplementedError(f'DBEngine not Supported: {uri_parts.scheme}')

        host = rationalize_docker_containers(host)
        return ConnectionInfo(engine, username, password, name, port, host)
