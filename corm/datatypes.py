import types
import typing

PWN = typing.TypeVar('PWN')

class Transliterator(typing.NamedTuple):
    cql_type: str
    python_to_cql: types.FunctionType
    cql_to_python: types.FunctionType
    values_encode_exemption: bool = False

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
