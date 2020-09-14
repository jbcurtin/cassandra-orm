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
    # TODO: Write Select logic

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

