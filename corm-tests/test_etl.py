import csv
import pytest
import random
import string

from datetime import datetime, timedelta

def generate_string(str_length: int) -> str:
    ascii_pool = string.ascii_letters + string.digits + string.punctuation
    return ''.join([random.choice(ascii_pool) for idx in range(0, str_length)])

@pytest.fixture(scope='function', autouse=True)
def setup_case(request):
    def destroy_case():
        from corm import annihilate_keyspace_tables
        annihilate_keyspace_tables('mykeyspace')

    request.addfinalizer(destroy_case)

def test__generate_sql_alchemy_table():
    from corm import register_table, insert, sync_schema
    from corm.models import CORMBase
    from corm.etl.utils import generate_sqlalchemy_metadata, generate_sqlalchemy_table

    from datetime import datetime

    from sqlalchemy import String, Float, BigInteger, Boolean, DateTime

    class TestModelDBInspect(CORMBase):
        __keyspace__ = 'mykeyspace'

        str_column: str = 1024
        second_str_column: str = 2048
        float_column: float
        int_column: int
        bool_column: bool
        timestamp_column: datetime

    sql_metadata = generate_sqlalchemy_metadata()
    sql_alchemy_table = generate_sqlalchemy_table(TestModelDBInspect, sql_metadata)
    assert sql_alchemy_table.columns['str_column'].type.__class__ is String
    assert sql_alchemy_table.columns['str_column'].type.length == 1024
    assert sql_alchemy_table.columns['str_column'].name == 'str_column'

    assert sql_alchemy_table.columns['second_str_column'].type.__class__ is String
    assert sql_alchemy_table.columns['second_str_column'].type.length == 2048
    assert sql_alchemy_table.columns['second_str_column'].name == 'second_str_column'

    assert sql_alchemy_table.columns['float_column'].type.__class__ is Float
    assert sql_alchemy_table.columns['float_column'].name == 'float_column'

    assert sql_alchemy_table.columns['int_column'].type.__class__ is BigInteger
    assert sql_alchemy_table.columns['int_column'].name == 'int_column'

    assert sql_alchemy_table.columns['bool_column'].type.__class__ is Boolean
    assert sql_alchemy_table.columns['bool_column'].name == 'bool_column'

    assert sql_alchemy_table.columns['timestamp_column'].type.__class__ is DateTime
    assert sql_alchemy_table.columns['timestamp_column'].name == 'timestamp_column'

    assert sql_alchemy_table.name == TestModelDBInspect.__name__

def test__convert_data_to_postgresql():
    import tempfile

    from corm import register_table, insert, sync_schema
    from corm.models import CORMBase

    from datetime import datetime

    class TestModelToPostgreSQL(CORMBase):
        __keyspace__ = 'mykeyspace'

        string_data: str
        float_data: float
        int_data: int
        text_data: str
        boolean_data: bool
        timestamp_data: datetime

    register_table(TestModelToPostgreSQL)
    sync_schema()
    insert_later = []
    for idx in range(0, 100):
        instance = TestModelToPostgreSQL(
                generate_string(10), random.uniform(0, 1),
                random.randint(0, 100), generate_string(2048),
                bool(random.randint(0, 1)), datetime.utcnow() + timedelta(seconds=random.randint(0, 10)))

        insert_later.append(instance)
        if len(insert_later) % 10 == 0:
            insert(insert_later)
            insert_later = []

    if insert_later:
        insert(insert_later)

    from corm.etl.utils import generate_sqlalchemy_metadata, generate_sqlalchemy_table, \
            sync_sqlalchemy_schema, migrate_data_to_sqlalchemy_table

    sql_metadata = generate_sqlalchemy_metadata()
    sql_table = generate_sqlalchemy_table(TestModelToPostgreSQL, sql_metadata)
    sync_sqlalchemy_schema(sql_metadata)
    migrate_data_to_sqlalchemy_table(TestModelToPostgreSQL, sql_table)

def test__cluster_uris_to_parts():
    from corm.etl.datatypes import ConnectionInfo, DBEngine
    from corm.etl.utils import cluster_uris_to_parts

    uris = [
        'cassandra://127.0.0.1:9042',
        'cassandra://127.0.0.1:9042/mykeyspace',
        'cassandra://username:@127.0.0.1:9042/mykeyspace',
        'cassandra://username:password@127.0.0.1:9042/mykeyspace',
    ]
    for idx, conn_info in enumerate(cluster_uris_to_parts(uris)):
        uri = uris[idx]
        assert conn_info.__class__ is ConnectionInfo
        if idx == 0:
            assert conn_info.engine is DBEngine.Cassandra
            assert conn_info.username is None
            assert conn_info.password is None
            assert conn_info.name == None
            assert conn_info.port == 9042
            assert conn_info.host == '127.0.0.1'
        elif idx == 1:
            assert conn_info.engine is DBEngine.Cassandra
            assert conn_info.username is None
            assert conn_info.password is None
            assert conn_info.name == 'mykeyspace'
            assert conn_info.port == 9042
            assert conn_info.host == '127.0.0.1'
        elif idx == 2:
            assert conn_info.engine is DBEngine.Cassandra
            assert conn_info.username == 'username'
            assert conn_info.password is None
            assert conn_info.name == 'mykeyspace'
            assert conn_info.port == 9042
            assert conn_info.host == '127.0.0.1'
        elif idx == 3:
            assert conn_info.engine is DBEngine.Cassandra
            assert conn_info.username == 'username'
            assert conn_info.password == 'password'
            assert conn_info.name == 'mykeyspace'
            assert conn_info.port == 9042
            assert conn_info.host == '127.0.0.1'
        
def test__export_to_csv_from_uri():
    import os
    import tempfile

    from corm import register_table, insert, sync_schema
    from corm.constants import ENCODING
    from corm.etl.constants import CASSANDRA_CONTAINER_NAME
    from corm.etl.datatypes import ConnectionInfo
    from corm.etl.utils import export_to_csv, run_command, container_ipaddress
    from corm.models import CORMBase

    class TestModelToCSV(CORMBase):
        __keyspace__ = 'mykeyspace'

        string_data: str
        float_data: float

    register_table(TestModelToCSV)
    sync_schema()
    insert_later = []
    for idx in range(0, 100):
        instance = TestModelToCSV(generate_string(10), random.uniform(0, 1))
        insert([instance])

    cassandra_ip = container_ipaddress(CASSANDRA_CONTAINER_NAME)
    filepath = tempfile.NamedTemporaryFile().name
    conn_info = ConnectionInfo.From_URI(f'cassandra://{cassandra_ip}:9042')
    assert not os.path.exists(filepath) or os.stat(filepath).st_size == 0
    export_to_csv(TestModelToCSV, filepath, conn_info)
    assert os.stat(filepath).st_size > 0
    os.remove(filepath)