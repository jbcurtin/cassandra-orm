Cassondra ORM ( CORM )
======================

Cassandra ORM which uses Python annotations simular to the typing.NamedTuple API

Install
-------

.. code-block:: python

    $ pip install -U corm


Usage
-----

.. code-block:: python

    import corm
    
    from corm.models import CORMBase
    
    from datetime import datetime
    
    class TestModel(CORMBase):
        __keyspace__ = 'mykeyspace'
        column_one: str
        column_two: int
        column_three: datetime
    
    
    corm.register_table(TestModel)
    corm.sync_schema()
    
    first_test = TestModel('one', 'two', datetime.utcnow())
    second_test = TestModel('first', 'second', datetime.utcnow())
    corm.insert([first_test, second_test])

.. toctree::
    :maxdepth: 2

