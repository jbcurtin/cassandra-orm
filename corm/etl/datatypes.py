import enum
import typing

from corm.constants import PWN

from urllib.parse import urlparse

class DBEngine(enum.Enum):
    PostgreSQL = 'postgresql'

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
        auth_info, net_info = uri_parts.netloc.split('@', 1)
        username, password = auth_info.split(':', 1)
        host, port = net_info.split(':', 1)
        name = uri_parts.path.strip('/')
        if uri_parts.scheme in ['postgres', 'postgresql']:
            engine = DBEngine.PostgreSQL

        return ConnectionInfo(engine, username, password, name, port, host)
