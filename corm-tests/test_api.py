import hashlib
import json
import types
import typing

from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

ENCODING = 'utf-8'
CLUSTER_IPS = ['127.0.0.1',]
CLUSTER_PORT = 9042
PWN = typing.TypeVar('PWN')
TABLES = {}
SESSIONS = {}
CLUSTER = Cluster(CLUSTER_IPS, port=CLUSTER_PORT)
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

def obtain_session(keyspace: str):
    if keyspace in SESSIONS.keys():
        return SESSIONS[keyspace]

    SESSIONS[keyspace] = CLUSTER.connect(keyspace)
    return SESSIONS[keyspace]

class Transliterator(typing.NamedTuple):
    cql_type: str
    python_to_cql: types.FunctionType
    cql_to_python: types.FunctionType
    values_encode_exemption: bool = False

def _python_to_cql__datetime(stamp: datetime) -> str:
    return stamp.strftime(DATETIME_FORMAT)

def _cql_to_python__datetime(stamp: str) -> datetime:
    return datetime.strptime(stamp, DATETIME_FORMAT)

DT_MAP = {
    str: Transliterator('TEXT', lambda x: str(x), lambda x: str(x)),
    int: Transliterator('BIGINT', lambda x: int(x), lambda x: int(x)),
    datetime: Transliterator('TIMESTAMP', _python_to_cql__datetime, _cql_to_python__datetime, True),
}

class CORMDetails(typing.NamedTuple):
    keyspace: str
    table_name: str
    field_names: typing.List[str]
    field_transliterators: typing.List[Transliterator]
    pk_fields: typing.List[str]

    def as_create_keyspace_cql(self: PWN) -> str:
        sql = [f'CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH REPLICATION = ']
        sql.append("{'class': 'NetworkTopologyStrategy', 'datacenter1': 3};")
        return ''.join(sql)

    def as_create_table_cql(self: PWN) -> str:
        entries = []
        for idx, field_name in enumerate(self.field_names):
            field_type = self.field_transliterators[idx].cql_type
            entry = f'{field_name} {field_type}'
            entries.append(entry)

        sql = [f'''CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.table_name} (''']
        sql.append(','.join(entries))
        sql.append(', guid TEXT PRIMARY KEY')
        sql.append(');')
        return ''.join(sql)

class CORMBase:
    def __init__(self: PWN, *args, **kwargs) -> None:
        for idx, (name, annotation) in enumerate(self.__annotations__.items()):
            setattr(self, name, args[idx])
            
    def as_hash(self: PWN) -> str:
        datum = {}
        for idx, field_name in enumerate(self._corm_details.field_names):
            value = getattr(self, field_name, None)
            if value is None:
                datum[field_name] = value

            else:
                datum[field_name] = self._corm_details.field_transliterators[idx].python_to_cql(value)

        sorted_datum = ''.join(sorted(json.dumps(datum)))
        return hashlib.sha256(sorted_datum.encode(ENCODING)).hexdigest()

    def values(self: PWN) -> typing.Tuple[typing.Any]:
        result = []
        for idx, field_name in enumerate(self._corm_details.field_names):
            if field_name in ['guid']:
                continue

            transliterator = self._corm_details.field_transliterators[idx]
            value = getattr(self, field_name, None)
            if value is None:
                result.append(None)

            elif transliterator.values_encode_exemption:
                result.append(value)

            else:
                result.append(transliterator.python_to_cql(value))

        return result

    def __repr__(self: PWN) -> None:
        formatted_pkfields = ','.join(self._corm_details.pk_fields)
        formatted_fields = ','.join(self._corm_details.field_names)
        return f'<{self.__class__.__name__}: [PKFields:{formatted_pkfields}] [Fields:{formatted_fields}]>'


class TestModel(CORMBase):
    __keyspace__ = 'mykeyspace'
    owner: str
    image: str
    tag: str
    created: datetime

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
    field_names = corm_objects[0]._corm_details.field_names
    field_names.append('guid')
    formatted_field_names = ','.join(field_names)
    formatted_question_marks = ','.join(['?' for idx in range(0, len(field_names))])
    CQL = f'INSERT INTO {keyspace}.{table_name} ({formatted_field_names}) VALUES ({formatted_question_marks})'
    prepared_statement = obtain_session(keyspace).prepare(CQL)
    cql_batch = BatchStatement()
    for corm_object in corm_objects:
        v_set = corm_object.values()
        v_set.append(corm_object.as_hash())
        cql_batch.add(prepared_statement, v_set)

    obtain_session(keyspace).execute(cql_batch)

register_table(TestModel)
sync_schema()

from datetime import datetime
entry_one = TestModel('owner', 'image', 'tag', datetime.utcnow())
entry_two = TestModel('owner two', 'image two', 'tag two', datetime.utcnow())
entry_three = TestModel('owner three', 'image three', 'tag three', datetime.utcnow())
insert([entry_one, entry_two, entry_three])
