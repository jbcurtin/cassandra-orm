ENCODING = 'utf-8'

def test_initial_api():
    from corm import register_table, insert, sync_schema
    from corm.models import CORMBase
    
    class TestModel(CORMBase):
        __keyspace__ = 'mykeyspace'
    
        something: str
        other: str
    
    register_table(TestModel)
    sync_schema()
    one = TestModel('one', 'two')
    two = TestModel('one', 'two')
    three = TestModel('one', 'three')
    insert([one, two, three])

def test_keyspace_api():
    import hashlib
    import uuid

    from corm import register_table, insert, sync_schema, \
            keyspace_exists, keyspace_destroy, keyspace_create
    from corm.datatypes import CassandraKeyspaceStrategy
    from corm.models import CORMBase

    # Keyspaces seem to have to start with Alpha-Letters
    keyspace_name = hashlib.md5(str(uuid.uuid4()).encode(ENCODING)).hexdigest()
    keyspace_name = f'abc_{keyspace_name}'
    assert keyspace_exists(keyspace_name) is False
    keyspace_create(keyspace_name, CassandraKeyspaceStrategy.Simple)
    assert keyspace_exists(keyspace_name) is True
    keyspace_destroy(keyspace_name)
    assert keyspace_exists(keyspace_name) is False

    class TestModelKeyspace(CORMBase):
        __keyspace__ = keyspace_name

        item: str

    register_table(TestModelKeyspace)
    assert keyspace_exists(keyspace_name) is False
    sync_schema()
    assert keyspace_exists(keyspace_name) is True
    one = TestModelKeyspace('one')
    insert([one])
    keyspace_destroy(keyspace_name)
    assert keyspace_exists(keyspace_name) is False

def test_float_api():
    from corm import register_table, insert, sync_schema, select
    from corm.models import CORMBase

    class TestModelFloat(CORMBase):
        __keyspace__ = 'mykeyspace'

        input_one: float

    register_table(TestModelFloat)
    sync_schema()
    one = TestModelFloat(2.4)
    insert([one])
    for idx, entry in enumerate(select(TestModelFloat)):
        assert entry.input_one == 2.4


def test_boolean_api():
    from corm import register_table, insert, sync_schema
    from corm.models import CORMBase

    from datetime import datetime

    class TestModelBoolean(CORMBase):
        __keyspace__ = 'mykeyspace'

        item: str
        created: datetime
        value: bool

    register_table(TestModelBoolean)
    sync_schema()
    one = TestModelBoolean('one', datetime.utcnow(), True)
    two = TestModelBoolean('two', datetime.utcnow(), False)
    insert([one, two])

def test_datetime_api():
    from corm import register_table, insert, sync_schema
    from corm.models import CORMBase

    from datetime import datetime

    class TestModelDatetime(CORMBase):
        __keyspace__ = 'mykeyspace'

        item: str
        created: datetime

    register_table(TestModelDatetime)
    sync_schema()
    one = TestModelDatetime('one', datetime.utcnow())
    two = TestModelDatetime('two', datetime.utcnow())
    insert([one, two])

def test_set_api():
    from corm import register_table, insert, sync_schema
    from corm.models import CORMBase
    from corm.annotations import Set

    class TestModelSet(CORMBase):
        __keyspace__ = 'mykeyspace'

        something: str
        other: Set

    register_table(TestModelSet)
    sync_schema()
    one = TestModelSet('one', {'first'})
    two = TestModelSet('two', {'last', 'second-to-last'})
    three = TestModelSet('three', {'last', 'second-to-last', 'last'})
    four = TestModelSet('four', ['one', 'two', 'three', 'four'])
    insert([one, two, three, four])

def test_select_api():
    import random

    from corm import register_table, insert, sync_schema, select
    from corm.models import CORMBase
    from corm.annotations import Set
    from datetime import datetime
    MAX_INT = 1000
    class TestModelSelect(CORMBase):
        __keyspace__ = 'mykeyspace'

        random_number: int
        created: datetime

    register_table(TestModelSelect)
    sync_schema()
    insert_later = []
    values = []
    for idx in range(0, 100):
        values.append({
            'random_number': random.randint(0, MAX_INT),
            'created': datetime.utcnow()
        })
        entry = TestModelSelect(values[-1]['random_number'], values[-1]['created'])
        insert_later.append(entry)
        if len(insert_later) > 20:
            insert(insert_later)
            insert_later = []

    insert(insert_later)
    for idx, entry in enumerate(select(TestModelSelect, fetch_size=100)):
        assert isinstance(entry, TestModelSelect)
        # Order is not consistent
        # assert entry.random_number == values[idx]['random_number']
        # assert entry.created == values[idx]['created']

    assert idx > 0

def test_alter_table_api():
    from corm import register_table, insert, sync_schema, select, obtain_session
    from corm.models import CORMBase
    from datetime import datetime

    # Create Table or Delete Column on existing Table
    class TestModelAlter(CORMBase):
        __keyspace__ = 'mykeyspace'

        random_number: int
        created: datetime

    register_table(TestModelAlter)
    sync_schema()

    COL_CQL = f'''
SELECT
    column_name, type
FROM
    system_schema.columns
WHERE
    table_name = '{TestModelAlter._corm_details.table_name}'
AND
    keyspace_name = '{TestModelAlter._corm_details.keyspace}'
'''
    rows = [(row.column_name, row.type) for row in obtain_session('mykeyspace').execute(COL_CQL)]
    assert len(rows) == 3

    # Add Column on existing Table
    class TestModelAlter(CORMBase):
        __keyspace__ = 'mykeyspace'

        random_number: int
        created: datetime
        new_column: str

    register_table(TestModelAlter)
    sync_schema()

    rows = [(row.column_name, row.type) for row in obtain_session('mykeyspace').execute(COL_CQL)]
    assert len(rows) == 4
