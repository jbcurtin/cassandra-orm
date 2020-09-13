
def test_whole_api():
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
    # TODO: Write Select logic
