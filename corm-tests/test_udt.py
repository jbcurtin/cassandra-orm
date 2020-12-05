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

def test_udt():
    from corm import register_table, register_user_defined_type, sync_schema, insert
    from corm.models import CORMUDTBase, CORMBase

    class TestUDTDatum(CORMUDTBase):
        __keyspace__ = 'mykeyspace'

        alpha: str
        beta: str

    class TestUDTModel(CORMBase):
        __keyspace__ = 'mykeyspace'
    
        something: str
        other: str
        udt_datum: TestUDTDatum

    register_user_defined_type(TestUDTDatum)
    register_table(TestUDTModel)
    sync_schema()
    one = TestUDTModel('one', 'two', TestUDTDatum('no', 'yes'))
    two = TestUDTModel('one', 'three', TestUDTDatum('yes', 'no'))
    insert([one, two])

