import hashlib
import typing

import ujson as json

from corm.constants import ENCODING

PWN = typing.TypeVar('PWN')

class CORMUDTBase:
    def __init__(self: PWN, *args, **kwargs) -> None:
        for idx, (name, annotation) in enumerate(self.__annotations__.items()):
            setattr(self, name, args[idx])

class CORMBase:
    def __init__(self: PWN, *args, **kwargs) -> None:
        for idx, (name, annotation) in enumerate(self.__annotations__.items()):
            setattr(self, name, args[idx])
            
    def as_hash(self: PWN) -> str:
        datum = {}
        for idx, field_name in enumerate(self._corm_details.field_names):
            value = getattr(self, field_name, None)
            if value is None:
                datum[field_name] = value

            else:
                if issubclass(value.__class__, CORMUDTBase):
                    datum[field_name] = {}
                    for udt_idx, udt_field_name in enumerate(value._udt_details.field_names):
                        udt_transliterator = value._udt_details.field_transliterators[udt_idx]
                        datum[field_name][udt_field_name] = udt_transliterator.python_to_cql(getattr(value, udt_field_name, None))

                else:
                    datum[field_name] = self._corm_details.field_transliterators[idx].python_to_cql(value)

        sorted_datum = ''.join(sorted(json.dumps(datum)))
        return hashlib.sha256(sorted_datum.encode(ENCODING)).hexdigest()

    def values(self: PWN) -> typing.Tuple[typing.Any]:
        result = []
        for idx, field_name in enumerate(self._corm_details.field_names):
            if field_name in ['guid']:
                continue

            transliterator = self._corm_details.field_transliterators[idx]
            value = getattr(self, field_name, None)
            if value is None:
                result.append(None)

            elif transliterator.values_encode_exemption:
                result.append(value)

            else:
                result.append(transliterator.python_to_cql(value))

        return result

    def __repr__(self: PWN) -> None:
        formatted_pkfields = ','.join(self._corm_details.pk_fields)
        formatted_fields = ','.join(self._corm_details.field_names)
        return f'<{self.__class__.__name__}: [PKFields:{formatted_pkfields}] [Fields:{formatted_fields}]>'
