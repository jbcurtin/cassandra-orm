import _io
import enum
import subprocess
import sys
import tempfile
import time
import types
import typing

from corm.constants import ENCODING, CLUSTER_PORT
from corm.etl.constants import PSQL_CLUSTER_PORT

class DBEngine(enum.Enum):
    PostgreSQL = ['postgres', 'postgresql', 'psql']
    Cassandra = ['cassandra']

def rationalize_docker_containers(ip_address: str, engine: DBEngine) -> str:
    if engine is DBEngine.PostgreSQL:
        cluster_port = PSQL_CLUSTER_PORT

    elif engine is DBEngine.Cassandra:
        cluster_port = CLUSTER_PORT

    else:
        raise NotImplementedError(engine)

    docker_hash = None
    if ip_address in ['127.0.0.1', 'localhost']:
        list_container_ips_cmd = 'docker container ls --format "table {{.ID}}: {{.Ports}}" -a'
        list_container_ips_filepath = tempfile.NamedTemporaryFile().name
        run_command(list_container_ips_cmd, True, list_container_ips_filepath)
        with open(list_container_ips_filepath, 'rb') as stream:
            container_ips = [ip.strip() for ip in stream.read().decode(ENCODING).split('\n') if ip]

        for entry in container_ips:
            match_label = f'0.0.0.0:{cluster_port}->{cluster_port}'
            if match_label in entry:
                docker_hash = entry.split(':', 1)[0]
                break

    if docker_hash is None:
        return ip_address

    try:
        return container_ipaddress(docker_hash) or ip_address
    except NotImplementedError:
        raise Exception

def run_command(cmd: typing.Union[str, typing.List[str]], shell: bool = True, stdout_filepath: str = None, throw_error: bool = True) -> None:
    if isinstance(cmd, str):
        cmd = [cmd]

    stdout_filepath = open(stdout_filepath, 'wb') if stdout_filepath else subprocess.PIPE
    proc = subprocess.Popen(cmd, shell=shell, stdout=stdout_filepath, stderr=subprocess.PIPE)

    while proc.poll() is None:
        time.sleep(.1)

    if proc.poll() > 0:
        sys.stderr.write(f'Exit Code Error[{proc.poll()}]')
        sys.stderr.write(proc.stderr.read().decode(ENCODING))
        if throw_error:
            raise OSError(proc.poll())

    if proc.stdout:
        sys.stdout.write(proc.stdout.read().decode(ENCODING))

    if stdout_filepath.__class__ is _io.BufferedWriter:
        stdout_filepath.close()

def container_ipaddress(container_name: str, container_network: str = 'bridge') -> str:
    cmd = f'docker inspect {container_name}|jq -r ".[0].NetworkSettings.Networks.{container_network}.IPAddress"'
    output_filepath = tempfile.NamedTemporaryFile().name
    run_command(cmd, True, output_filepath)
    with open(output_filepath, 'rb') as stream:
        value = stream.read().decode(ENCODING).strip('\n ')
        if value == 'null':
            raise NotImplementedError(f"Container doesn't exist: {container_name}")

        return value
