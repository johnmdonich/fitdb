from abc import ABC
from typing import Any, TypeAlias, TypeVar

from polars import DataFrame

listified_alias: TypeAlias = type[list[type[Any]]]


def listify_annotations(cls: type) -> dict[str, listified_alias]:
    annotations: dict[str, type[Any]] = cls.__annotations__
    listified_annotations: dict[str, type[Any]] = {}
    for member, member_type in annotations.items():
        listified_annotations[member] = list[member_type]  # type: ignore
    return listified_annotations


class Columnar(ABC):
    __slots__: tuple[str, ...] = ()
    __annotations__ = {}

    def __init__(self) -> None:
        for slot in self.__slots__:
            setattr(self, slot, [])

    def append(self, row: Any) -> None:
        for slot in self.__slots__:
            getattr(self, slot).append(getattr(row, slot))

    def asdict(self) -> dict[str, Any]:
        return {slot: getattr(self, slot) for slot in self.__slots__}

    def df(self) -> DataFrame:
        return DataFrame(self.asdict())
