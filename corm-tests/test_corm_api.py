import pytest

ENCODING = 'utf-8'

@pytest.fixture(scope='function', autouse=True)
def setup_case(request):
    def destroy_case():
        from corm import annihilate_keyspace_tables, SESSIONS
        annihilate_keyspace_tables('mykeyspace')
        for keyspace_name, session in SESSIONS.copy().items():
            if keyspace_name in ['global']:
                continue

            session.shutdown()
            del SESSIONS[keyspace_name]


    request.addfinalizer(destroy_case)

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
    data = 324.593998934
    one = TestModelFloat(data)
    insert([one])
    for idx, entry in enumerate(select(TestModelFloat)):
        assert entry.input_one == data

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

def test_select_where_api():
    import random

    from corm import register_table, insert, sync_schema, select, where
    from corm.models import CORMBase
    from datetime import datetime

    MAX_INT = 99999
    class TestModelSelectSource(CORMBase):
        __keyspace__ = 'mykeyspace'

        random_number: int
        created: datetime
        one: str
        two: str

    class TestModelSelectPivot(CORMBase):
        __keyspace__ = 'mykeyspace'

        random_number: int
        created: datetime
        one: str
        two: str
        source: TestModelSelectSource

    # TODO: Build UserType integration
    # register_table(TestModelSelectSource)
    # register_table(TestModelSelectPivot)

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

def test_not_ordered_by_pk_field():
    import random

    from corm import register_table, insert, sync_schema, select, obtain_session
    from corm.models import CORMBase
    from datetime import datetime

    class TestNotOrderedByPkField(CORMBase):
        __keyspace__ = 'mykeyspace'
        __primary_keys__ = ['one', 'two', 'three']

        random_number: int
        created: datetime
        one: str
        two: str
        three: str

    register_table(TestNotOrderedByPkField)
    sync_schema()

    first_entry = TestNotOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'beta')
    gamma = TestNotOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'gamma')
    delta = TestNotOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'delta')
    second_entry = TestNotOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'alpha')
    insert([first_entry, gamma, delta, second_entry])
    for idx, entry in enumerate(select(TestNotOrderedByPkField)):
        if idx == 0:
            assert entry.three != 'alpha'

def test_ordered_by_pk_field():
    import random

    from corm import register_table, insert, sync_schema, select, obtain_session
    from corm.models import CORMBase
    from corm.datatypes import TableOrdering
    from datetime import datetime

    class TestOrderedByPkField(CORMBase):
        __keyspace__ = 'mykeyspace'
        __primary_keys__ = ['one', 'two', 'three']
        __ordered_by_primary_keys__ = TableOrdering.DESC

        random_number: int
        created: datetime
        one: str
        two: str
        three: str

    register_table(TestOrderedByPkField)
    sync_schema()

    first_entry = TestOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'beta')
    second_entry = TestOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'alpha')
    gamma = TestOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'gamma')
    delta = TestOrderedByPkField(random.randint(0, 99999), datetime.utcnow(), 'one', 'one', 'delta')
    insert([first_entry, second_entry, delta, gamma])
    for idx, entry in enumerate(select(TestOrderedByPkField)):
        if idx == 0:
            assert entry.three == 'alpha'

        elif idx == 1:
            assert entry.three == 'beta'

        elif idx == 2:
            assert entry.three == 'delta'

        elif idx == 3:
            assert entry.three == 'gamma'

def test_corm_auth():
    import os
    os.environ['CLUSTER_PORT'] = '9043'
    os.environ['CLUSTER_USERNAME'] = 'cassandra'
    os.environ['CLUSTER_PASSWORD'] = 'cassandra'

    from corm import register_table, insert, sync_schema
    from corm.models import CORMBase

    class TestCORMAuth(CORMBase):
        one: str
        __keyspace__ = 'test_corm_auth'

    register_table(TestCORMAuth)
    sync_schema()
