import typing

from corm.constants import CLUSTER_IPS, CLUSTER_PORT
from corm.encoders import DT_MAP
from corm.datatypes import CORMDetails

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

TABLES = {}
SESSIONS = {}
CLUSTER = Cluster(CLUSTER_IPS, port=CLUSTER_PORT)

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
    cmds_by_keyspace = {}
    for table_name, table in TABLES.items():
        cmds = cmds_by_keyspace.get(table.keyspace, [])
        cmds.append(table.as_create_keyspace_cql())
        cmds.append(table.as_create_table_cql())
        cmds_by_keyspace[table.keyspace] = cmds

    for keyspace, cmds in cmds_by_keyspace.items():
        session = obtain_session(keyspace)
        for cmd in cmds:
            session.execute(cmd)

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
