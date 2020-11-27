import _io
import logging
import subprocess
import sys
import tempfile
import time
import types
import typing

from corm.constants import PWN, CLUSTER_IPS, ENCODING
from corm.etl.constants import POSTGRESQL_URI, CASSANDRA_CONTAINER_NAME, POSTRGESQL_CONTAINER_NAME, \
        ETL_CLUSTER_URIS
from corm.etl.datatypes import ConnectionInfo
from corm.etl.helpers import run_command, container_ipaddress
from corm.models import CORMBase
from corm.annotations import Set

from datetime import datetime

from sqlalchemy import String, BigInteger, DateTime, ARRAY, Boolean, Float, Column, Table, MetaData, \
        create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine

logger = logging.getLogger(__name__)

PSQL_SESSIONS = {}
DT_SQLALCHEMY_MAP_POSTGRESQL = {
    str: String,
    int: BigInteger,
    datetime: DateTime,
    Set: ARRAY,
    bool: Boolean,
    float: Float,
}
class session:
    _engine: Engine
    _manager: sessionmaker
    @property
    def connection(self: PWN) -> Engine:
        return self._engine

    def __init__(self: PWN, engine: Engine) -> None:
        self._engine = engine
        self._manager = sessionmaker(engine)

    def __enter__(self: PWN) -> 'Session':
        self._session = self._manager()
        return self._session

    def __exit__(self: PWN, *args, **kwargs) -> typing.Any:
        self._session.close()
        self._session = None
        self._manager = None

def obtain_sqlalchemy_session(uri: str) -> Engine:
    if uri is None:
        raise NotImplementedError(f'Unable to load URI')

    if PSQL_SESSIONS.get(uri, None) is None:
        engine = create_engine(uri)
        PSQL_SESSIONS[uri] = session(engine)

    return PSQL_SESSIONS[uri]

def generate_sqlalchemy_metadata() -> MetaData:
    return MetaData(bind=obtain_sqlalchemy_session(POSTGRESQL_URI).connection)

def generate_sqlalchemy_table(table: CORMBase, metadata: MetaData) -> Table:
    sql_alchemy_types = {}
    for field_name, field_type in table.__annotations__.items():
        default_value = getattr(table, field_name, None)
        if default_value:
            sql_alchemy_types[field_name] = DT_SQLALCHEMY_MAP_POSTGRESQL[field_type](default_value)

        else:
            sql_alchemy_types[field_name] = DT_SQLALCHEMY_MAP_POSTGRESQL[field_type]

    sql_alchemy_types['guid'] = String(65)
    cols = [Column(field_name, field_type) for field_name, field_type in sql_alchemy_types.items()]
    return Table(table.__name__.lower(), metadata, *cols)

def sync_sqlalchemy_schema(sql_metadata: MetaData) -> None:
    sql_metadata.create_all()

def cluster_uris_to_parts(uris: typing.List[str]) -> types.GeneratorType:
    for uri in uris:
        yield ConnectionInfo.From_URI(uri)

def _corm_table_to_cql_export(table: CORMBase) -> str:
    field_names = table._corm_details.field_names[:]
    field_names.append('guid')
    formatted_field_names = ','.join(field_names)
    return f"""COPY {table._corm_details.keyspace}.{table._corm_details.table_name} ({formatted_field_names}) TO STDOUT WITH HEADER=True AND QUOTE=\'*\' AND ESCAPE=\'*\' """

def export_to_csv(table: CORMBase, filepath: str, info: ConnectionInfo) -> None:
    cql_export = _corm_table_to_cql_export(table)
    cql_bin_cmd = f'docker run --rm library/cassandra cqlsh {info.host} {info.port}'
    export_cmd = f'{cql_bin_cmd} -e "{cql_export}" > {filepath}'
    logger.info(f'Exporting Table[{table._corm_details.table_name}] from {info.host}')
    run_command(export_cmd)

def migrate_data_to_sqlalchemy_table(corm_table: CORMBase, sql_table: Table) -> None:
    cassandra_ipaddress = container_ipaddress(CASSANDRA_CONTAINER_NAME)
    psql_ipaddress = container_ipaddress(POSTRGESQL_CONTAINER_NAME)

    # Export data from Cassandra
    field_names = corm_table._corm_details.field_names[:]
    field_names.append('guid')
    formatted_field_names = ','.join(field_names)
    CQL_EXPORT = f"""COPY {corm_table._corm_details.keyspace}.{corm_table._corm_details.table_name} ({formatted_field_names}) TO STDOUT WITH HEADER=True AND QUOTE=\'*\' AND ESCAPE=\'*\' """
    CQL_BIN_CMD = f'docker run --rm cassandra cqlsh {cassandra_ipaddress} 9042'
    csv_filepath = tempfile.NamedTemporaryFile().name
    export_cmd = f'{CQL_BIN_CMD} -e "{CQL_EXPORT}" > {csv_filepath}'
    logger.info(f'Exporting Data from {CASSANDRA_CONTAINER_NAME}')
    run_command(export_cmd)

    # Import data into PostgreSQL
    column_names = [col.name for col in sql_table.columns]
    formatted_columns = ','.join(column_names)
    SQL_IMPORT = f"""COPY {sql_table.name} ({formatted_columns}) FROM STDIN DELIMITER ',' CSV HEADER QUOTE AS \'*\' ESCAPE AS \'*\' """
    postgresql_info = ConnectionInfo.From_URI(POSTGRESQL_URI)
    PSQL_BIN_CMD = f'docker run -i -e PGPASSWORD="{postgresql_info.password}" --rm postgres psql -h {psql_ipaddress} -U {postgresql_info.username} {postgresql_info.name}'
    import_cmd = f'{PSQL_BIN_CMD} -c "{SQL_IMPORT}" < {csv_filepath}'
    logger.info(f'Importing data into {CASSANDRA_CONTAINER_NAME}')
    run_command(import_cmd)

def rationalize_docker_containers(ip_address: str) -> str:
    if ip_address in ['127.0.0.1', 'localhost']:
        import pdb; pdb.set_trace()
        pass

    return ip_address
