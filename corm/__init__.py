import enum
import logging
import typing

from corm.constants import CLUSTER_IPS, CLUSTER_PORT, PWN
from corm.annotations import Set
from corm.auth import AuthProvider
from corm.encoders import DT_MAP, UDT_MAP, setup_udt_transliterator
from corm.models import CORMBase, CORMUDTBase
from corm.datatypes import CORMDetails, CassandraKeyspaceStrategy, TableOrdering, CORMUDTDetails, EnumTransliterator

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

UDT_TYPES = {}
TABLES = {}
SESSIONS = {}
if AuthProvider:
    CLUSTER = Cluster(CLUSTER_IPS, port=CLUSTER_PORT, auth_provider=AuthProvider)
else:
    CLUSTER = Cluster(CLUSTER_IPS, port=CLUSTER_PORT)

SESSIONS['global'] = CLUSTER.connect()
RESERVED_KEYSPACE_NAMES = ['global']

logger = logging.getLogger(__name__)

def obtain_session(keyspace_name: str, auto_create_keyspace: bool = False) -> 'SESSIONS[keyspace_name]':
    if keyspace_name in RESERVED_KEYSPACE_NAMES:
        raise NotImplementedError(f'Unable to request Keyspace Name[{keyspace_name}]')

    if keyspace_name in SESSIONS.keys():
        return SESSIONS[keyspace_name]

    try:
        SESSIONS[keyspace_name] = CLUSTER.connect(keyspace_name)
    except Exception as err:
        if auto_create_keyspace:
            keyspace_create(keyspace_name, CassandraKeyspaceStrategy.Simple)
            SESSIONS[keyspace_name] = CLUSTER.connect(keyspace_name)

        else:
            raise err

    return SESSIONS[keyspace_name]

def keyspace_exists(keyspace_name: str) -> None:
    CQL = f"""SELECT
    keyspace_name,
    durable_writes,
    replication
FROM system_schema.keyspaces
WHERE keyspace_name = '{keyspace_name}';"""

    rows = [row for row in SESSIONS['global'].execute(CQL)]
    if len(rows) == 0:
        return False

    return True

def keyspace_create(keyspace_name: str, strategy: CassandraKeyspaceStrategy) -> None:
    if strategy is CassandraKeyspaceStrategy.Simple:
        CQL = "CREATE KEYSPACE %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};" % keyspace_name
    else:
        raise NotImplementedError

    SESSIONS['global'].execute(CQL)

def keyspace_destroy(keyspace_name: str) -> None:
    CQL = "DROP KEYSPACE IF EXISTS %s" % keyspace_name
    SESSIONS['global'].execute(CQL)

def annihilate_keyspace_tables(keyspace_name: str) -> None:
    FIND_TABLES_CQL = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace_name}';"
    for row in SESSIONS['global'].execute(FIND_TABLES_CQL):
        cql = f'DROP TABLE IF EXISTS {keyspace_name}.{row.table_name};'
        SESSIONS['global'].execute(cql)

def register_user_defined_type(udt: CORMUDTBase) -> None:
    keyspace = getattr(udt, '__keyspace__', None)
    if keyspace is None:
        raise NotImplementedError(f'Table[{udt.__class__}] missing Keyspace')

    field_names = []
    field_transliterators = []
    for field_name, annotation in udt.__annotations__.items():
        field_names.append(field_name)
        field_transliterators.append(DT_MAP[annotation])

    udt_details = CORMUDTDetails(
            udt.__keyspace__,
            udt.__name__.lower(),
            getattr(udt, '__udt_key__', udt.__name__.lower()),
            field_names,
            field_transliterators)

    udt._udt_details = udt_details
    UDT_TYPES[udt_details.name] = udt
    setup_udt_transliterator(udt)

def register_table(table: typing.NamedTuple) -> None:
    keyspace = getattr(table, '__keyspace__', None)
    if keyspace is None:
        raise NotImplementedError(f'Table[{table.__class__}] missing Keyspace')

    field_names = []
    field_transliterators = []
    for field_name, annotation in table.__annotations__.items():
        field_names.append(field_name)
        try:
            transliterator = DT_MAP[annotation]
        except KeyError:
            if isinstance(annotation, Set):
                import pdb; pdb.set_trace()
                pass

            elif issubclass(annotation, enum.Enum):
                transliterator = EnumTransliterator(annotation)

            else:
                transliterator = UDT_MAP.get(annotation, None)
                if transliterator is None:
                    raise NotImplementedError

        field_transliterators.append(transliterator)

    pk_fields = getattr(table, '__primary_keys__', [])[:] or field_names[:]
    for pk_field in pk_fields:
        if not pk_field in field_names:
            raise NotImplementedError(f'Field[{pk_field}] not in Table[{table.__class__}]')

    ordered_by_primary_keys = getattr(table, '__ordered_by_primary_keys__', TableOrdering.Nope)
    assert ordered_by_primary_keys.__class__ is TableOrdering, 'Invalid Datatype. Using corm.datatypes.TableOrdering object'

    corm_details = CORMDetails(
        table.__keyspace__,
        table.__name__.lower(),
        field_names,
        field_transliterators,
        pk_fields,
        ordered_by_primary_keys)

    TABLES[corm_details.table_name] = corm_details
    table._corm_details = corm_details

def sync_schema() -> None:
    """
    https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useQuerySystemTable.html
    """
    # Sync User Defined Types, then tables
    keyspace_udts = {}
    for udt_name, udt in UDT_TYPES.items():
        udts = keyspace_udts.get(udt._udt_details.keyspace, [])
        udts.append(udt)
        keyspace_udts[udt._udt_details.keyspace] = udts

    for udt_idx, (udt_keyspace_name, udts) in enumerate(keyspace_udts.items()):
        if keyspace_exists(udt_keyspace_name) is False:
            keyspace_create(udt_keyspace_name, True)

        session = obtain_session(udt_keyspace_name, True)
        for user_defined_type in udts:
            session.execute(user_defined_type._udt_details.as_create_user_defined_type_cql())
            CLUSTER.register_user_type(user_defined_type._udt_details.keyspace, udt._udt_details.udt_key, user_defined_type)

            # (CLUSTER, udt)

    # Create or Update Tables
    keyspace_tables = {}
    for table_name, table in TABLES.items():
        tables = keyspace_tables.get(table.keyspace, [])
        tables.append(table)
        keyspace_tables[table.keyspace] = tables


    for idx, (keyspace_name, tables) in enumerate(keyspace_tables.items()):
        if keyspace_exists(keyspace_name) is False:
            keyspace_create(keyspace_name, CassandraKeyspaceStrategy.Simple)

        session = obtain_session(keyspace_name, True)
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
                logger.info(f'Creating Table[{table.table_name}] in Keyspace[{table.keyspace}]')
                session.execute(table.as_create_table_cql())

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
    prepared_statement = obtain_session(keyspace, True).prepare(CQL)
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

    def __next__(self: PWN) -> CORMBase:
        if len(self._fetched) < 1:
            if self._iter.has_more_pages is False:
                raise StopIteration

            self._iter.fetch_next_page()
            self._fetched.extend(self._iter.current_rows)

        if len(self._fetched) < 1:
            raise StopIteration

        next_object = self._fetched.pop()
        values = []
        for field_name, annotation in self._table.__annotations__.items():
            try:
                transliterator = DT_MAP[annotation]
            except KeyError:
                if issubclass(annotation, enum.Enum):
                    transliterator = EnumTransliterator(annotation)

                else:
                    transliterator = UDT_MAP.get(annotation, None)
                    if transliterator is None:
                        raise NotImplementedError

            raw_value = getattr(next_object, field_name, None)
            values.append(transliterator.cql_to_python(raw_value))

        return self._table(*values)

class Operator(enum.Enum):
    Equal = 'equal'

class cp:
    def __init__(self: PWN, operator: Operator, field_name: str, value: typing.Any) -> None:
        self._operator = operator
        self._field_name = field_name
        self._value = value

    def as_cql(self: PWN, table: CORMBase) -> str:
        assert self._field_name in table.__annotations__.keys(), f'Field[{self._field_name}] not available on Table[{table}]'
        if isinstance(self._value, (float, int)):
            return f"{self._field_name} = {self._value}"

        elif issubclass(self._value.__class__, enum.Enum):
            return f"{self._field_name} = '{self._value.value}'"

        elif isinstance(self._value, str):
            return f"{self._field_name} = '{self._value}'"

        else:
            raise NotImplementedError(self._value.__class__)

class where(select):
    def __init__(self: PWN, table: CORMBase, compare_functions: typing.List[cp], field_names: typing.List[str] = [], fetch_size: int = 100, limit: int = 0) -> None:
        self._table = table
        self._field_names = field_names or table._corm_details.field_names

        formatted_field_names = ','.join(self._field_names)
        keyspace = self._table._corm_details.keyspace
        table_name = self._table._corm_details.table_name

        # select * from marketstack_com.history where symbol = 'LTUU' limit 3 ALLOW FILTERING
        self._query = f'SELECT {formatted_field_names} FROM {keyspace}.{table_name}'
        where_clause = ' AND'.join([cp_func.as_cql(table) for cp_func in compare_functions])
        if where_clause:
            self._query = f'{self._query} WHERE {where_clause}'

        if limit > 0:
            self._query = f'{self._query} LIMIT = {limit}'

        if where_clause:
            self._query = f'{self._query} ALLOW FILTERING'

        self._fetch_size = fetch_size
        self._stmt = SimpleStatement(self._query, fetch_size=fetch_size)
        self._iter = obtain_session(self._table._corm_details.keyspace).execute(self._stmt)
        self._fetched = []
        self._fetched.extend(self._iter.current_rows)
