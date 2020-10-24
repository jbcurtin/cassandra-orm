import enum
import types
import typing

PWN = typing.TypeVar('PWN')

class CassandraKeyspaceStrategy(enum.Enum):
    Simple: str = 'SimpleStrategy'

class Transliterator(typing.NamedTuple):
    cql_type: str
    python_to_cql: types.FunctionType
    cql_to_python: types.FunctionType
    values_encode_exemption: bool = False

class TableOrdering(enum.Enum):
    DESC = 'desc'
    ASC = 'asc'
    Nope = 'nope'

class CORMDetails(typing.NamedTuple):
    keyspace: str
    table_name: str
    field_names: typing.List[str]
    field_transliterators: typing.List[Transliterator]
    pk_fields: typing.List[str]
    ordered_by_primary_keys: TableOrdering

    def as_create_table_cql(self: PWN) -> str:
        entries = []
        for idx, field_name in enumerate(self.field_names):
            field_type = self.field_transliterators[idx].cql_type
            entry = f'{field_name} {field_type}'
            entries.append(entry)

        cql = [f'''CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.table_name} (''']
        cql.append(','.join(entries))
        if not self.ordered_by_primary_keys is TableOrdering.Nope:
            formatted_pk_fields = ','.join(self.pk_fields[:-1])
            formatted_pk_fields = f'({formatted_pk_fields})'
            sort_field = self.pk_fields[-1]
            cql.append(', guid TEXT')
            cql.append(f', PRIMARY KEY({formatted_pk_fields}, {sort_field})')
            cql.append(')')
            cql.append(f' WITH CLUSTERING ORDER BY ({sort_field} {self.ordered_by_primary_keys.value});')
        else:
            cql.append(', guid TEXT PRIMARY KEY')
            cql.append(');')

        return ''.join(cql)
