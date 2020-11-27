from corm.models import CORMBase

class RandomDataModel(CORMBase):
    __keyspace__ = 'corm_factory_keyspace'

    random_string: str
    random_float: float
