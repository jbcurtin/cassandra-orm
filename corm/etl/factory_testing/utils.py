import importlib
import random
import types

from corm.models import CORMBase
from corm.utils import generate_string

LOADED_TABLES = {}
def load_table(table_path: str) -> CORMBase:
    if table_path in LOADED_TABLES.keys():
        return LOADED_TABLES[table_path]

    module_path, table_name = table_path.rsplit('.', 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError:
        raise ImportError(f'Unable to load table: {table_path}')

    corm_table = getattr(module, table_name, None)
    if corm_table is None:
        raise ImportError(f'Unable to load table: {table_path}')

    LOADED_TABLES[table_path] = corm_table
    return LOADED_TABLES[table_path]

def generate_entries(table: CORMBase, count: int = 100) -> types.GeneratorType:
    for idx in range(0, count):
        field_values = []
        for field_idx, field_name in enumerate(table._corm_details.field_names):
            field_transliterator = table._corm_details.field_transliterators[field_idx]
            if field_transliterator.python_type is str:
                field_values.append(generate_string(10))

            elif field_transliterator.python_type is int:
                field_values.append(random.randint(0, 10))

            elif field_transliterator.python_type is float:
                field_values.append(random.uniform(0, 1))

            else:
                raise NotImplementedError(field_transliterator.python_type)

        yield table(*field_values)
