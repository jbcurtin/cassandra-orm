# Cassondra ORM ( CORM )

Cassandra ORM which uses Python annotations similar to the typing.NamedTuple API

## Install

```
$ pip install -U corm
```

## Usage

```
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
```
