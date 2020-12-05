from corm.annotations import Set
from corm.constants import DATETIME_FORMAT
from corm.datatypes import Transliterator
from corm.models import CORMUDTBase

from datetime import datetime

def datetime__python_to_cql(stamp: datetime) -> str:
    return stamp.strftime(DATETIME_FORMAT)

def datetime__cql_to_python(stamp: str) -> datetime:
    return datetime.strptime(stamp, DATETIME_FORMAT)

DT_MAP = {
    str: Transliterator(str, 'TEXT', lambda x: str(x), lambda x: str(x)),
    int: Transliterator(int, 'BIGINT', lambda x: int(x), lambda x: int(x)),
    datetime: Transliterator(datetime, 'TIMESTAMP', datetime__python_to_cql, datetime__cql_to_python, True),
    Set: Transliterator(Set, 'SET<text>', lambda x: [i for i in x], lambda x: x),
    bool: Transliterator(bool, 'BOOLEAN', lambda x: x, lambda x: x),
    float: Transliterator(float, 'DOUBLE', lambda x: x, lambda x: x),
}

UDT_MAP = {}
def setup_udt_transliterator(udt: CORMUDTBase) -> Transliterator:
    if udt._udt_details.udt_key in UDT_MAP.keys():
        raise NotImplementedError(f'Duplicate Transliterator[{udt}]')

    UDT_MAP[udt] = Transliterator(udt, udt._udt_details.udt_key, lambda x: x, lambda x: x)
    return UDT_MAP[udt]
