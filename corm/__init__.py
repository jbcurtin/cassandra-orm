import logging
import typing

from corm.constants import CLUSTER_IPS, CLUSTER_PORT, PWN
from corm.encoders import DT_MAP
from corm.models import CORMBase
from corm.datatypes import CORMDetails

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

TABLES = {}
SESSIONS = {}
CLUSTER = Cluster(CLUSTER_IPS, port=CLUSTER_PORT)

logger = logging.getLogger(__name__)

def obtain_session(keyspace: str):
    if keyspace in SESSIONS.keys():
        return SESSIONS[keyspace]

    SESSIONS[keyspace] = CLUSTER.connect(keyspace)
    return SESSIONS[keyspace]

def register_table(table: typing.NamedTuple) -> None:
    keyspace = getattr(table, '__keyspace__', None)
    if keyspace is None:
        raise NotImplementedError(f'Table[{table.__class__}] missing Keyspace')

    field_names = []
    field_transliterators = []
    for field_name, annotation in table.__annotations__.items():
        field_names.append(field_name)
        field_transliterators.append(DT_MAP[annotation])

    pk_fields = getattr(table, 'primary_key', None) or field_names[:]
    for pk_field in pk_fields:
        if not pk_field in field_names:
            raise NotImplementedError(f'Field[{pk_field}] not in Table[{table.__class__}]')

    corm_details = CORMDetails(
        table.__keyspace__,
        table.__name__.lower(),
        field_names,
        field_transliterators,
        pk_fields)

    TABLES[corm_details.table_name] = corm_details
    table._corm_details = corm_details

def sync_schema() -> None:
    """
    https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useQuerySystemTable.html
    """
    keyspace_tables = {}
    for table_name, table in TABLES.items():
        tables = keyspace_tables.get(table.keyspace, [])
        tables.append(table)
        keyspace_tables[table.keyspace] = tables

    for keyspace_name, tables in keyspace_tables.items():
        obtain_session(keyspace_name).execute(table.as_create_keyspace_cql())
        session = obtain_session(keyspace_name)
        for table in tables:
            COLUMN_CQL = f'''
            SELECT
                column_name, type
            FROM
                system_schema.columns
            WHERE table_name = ?
                AND keyspace_name = ?'''
            stmt = obtain_session(keyspace_name).prepare(COLUMN_CQL)
            existing_columns = {r.column_name: r.type for r in obtain_session(keyspace_name).execute(stmt, [table.table_name, keyspace_name])}
            # Add Whole Table
            if len(existing_columns.keys()) == 0:
                logger.info(f'Creating Table[{table.table_name}]')
                obtain_session(keyspace_name).execute(table.as_create_table_cql())

            # Add Columns
            elif len(table.field_names) > len(existing_columns.keys()) - 1:
                column_updates = {}
                for field_idx, field_name in enumerate(table.field_names):
                    field_transliterator = table.field_transliterators[field_idx]
                    if field_name in existing_columns.keys():
                        if field_transliterator.cql_type.lower() != existing_columns[field_name].lower():
                            raise NotImplementedError

                    else:
                        column_updates[field_name] = field_transliterator.cql_type

                formatted_column_names = ', '.join(sorted(column_updates.keys()))
                formatted_column_definitions = ',\n'.join([' '.join([c_name, c_type]) for c_name, c_type in column_updates.items()])
                logger.info(f'Altering Table[{table.table_name}]. Adding Columns[{formatted_column_names}]')
                ALTER_CQL = f'''
ALTER TABLE
    {keyspace_name}.{table.table_name}
ADD ({formatted_column_definitions})
'''
                obtain_session(keyspace_name).execute(ALTER_CQL)

            # Delete Columns
            elif len(table.field_names) < len(existing_columns.keys()) - 1:
                columns_to_be_removed = [key for key in existing_columns.keys() if not key in table.field_names and key != 'guid']
                formatted_column_names = ', '.join(sorted(columns_to_be_removed))
                ALTER_CQL = f'''
ALTER TABLE
    {keyspace_name}.{table.table_name}
DROP ({formatted_column_names})
'''
                obtain_session(keyspace_name).execute(ALTER_CQL)

def insert(corm_objects: typing.List[typing.Any]) -> None:
    keyspace = corm_objects[0]._corm_details.keyspace
    table_name = corm_objects[0]._corm_details.table_name
    field_names = corm_objects[0]._corm_details.field_names[:]
    instance_type = corm_objects[0].__class__
    field_names.append('guid')
    formatted_field_names = ','.join(field_names)
    formatted_question_marks = ','.join(['?' for idx in range(0, len(field_names))])
    CQL = f'INSERT INTO {keyspace}.{table_name} ({formatted_field_names}) VALUES ({formatted_question_marks})'
    prepared_statement = obtain_session(keyspace).prepare(CQL)
    cql_batch = BatchStatement()
    for corm_object in corm_objects:
        if corm_object.__class__ != instance_type:
            raise Exception('All corm_objects must be the same type')

        v_set = corm_object.values()
        v_set.append(corm_object.as_hash())
        cql_batch.add(prepared_statement, v_set)

    obtain_session(keyspace).execute(cql_batch)

class select:
    def __init__(self: PWN, table: CORMBase, field_names: typing.List[str] = [], fetch_size: int = 100) -> None:
        self._table = table
        self._field_names = field_names or table._corm_details.field_names

        formatted_field_names = ','.join(self._field_names)
        keyspace = self._table._corm_details.keyspace
        table_name = self._table._corm_details.table_name
        self._query = f'SELECT {formatted_field_names} FROM {keyspace}.{table_name}'
        self._fetch_size = fetch_size
        self._stmt = SimpleStatement(self._query, fetch_size=fetch_size)
        self._iter = obtain_session(self._table._corm_details.keyspace).execute(self._stmt)
        self._fetched = []
        self._fetched.extend(self._iter.current_rows)

    def __iter__(self: PWN) -> PWN:
        return self

    def __next__(self: PWN) -> typing.Any:
        if len(self._fetched) < 1:
            if self._iter.has_more_pages is False:
                raise StopIteration

            self._iter.fetch_next_page()
            self._fetched.extend(self._iter.current_rows)

        return self._fetched.pop()
