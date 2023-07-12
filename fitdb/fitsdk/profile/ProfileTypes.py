from abc import ABC
from typing import Any, TypedDict, TypeAlias

from polars import DataFrame

from ..Columnar import Columnar, listify_annotations


def build_int_array_object(array: list[int | str] | None) -> list[int | None] | None:
    if array is None or len(array) == 0 or array[0] == "":
        return None
    return [int(val) if val != "" else None for val in array]


def build_str_array_object(array: str | list[str] | None) -> list[str | None] | None:
    if array is None or len(array) == 0 or array[0] == "":
        return None
    elif isinstance(array, str):
        return [array]
    return [str(val) if val != "" else None for val in array]


class Message:
    num: int
    name: str
    messages_key: str
    __slots__ = tuple(__annotations__.keys())

    def __init__(self, num: int, name: str, messages_key: str):
        self.num = int(num)
        self.name = str(name)
        self.messages_key = str(messages_key)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.num}, {self.name}, {self.messages_key})"
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: (num: {self.num}, name: {self.name}, message_key: {self.messages_key})"


class Messages(Columnar):
    __slots__ = Message.__slots__
    __annotations__ = listify_annotations(Message)


class MessageField:
    num: int
    name: str
    type: str | None
    is_array: bool
    scale: list[int | None] | None
    offset: list[int | None] | None
    units: list[str | None] | None
    bits: list[int | None] | None
    components: list[int | None] | None
    is_accumulated: bool
    has_components: bool
    message_num: int | None
    __slots__ = tuple(__annotations__.keys())

    def __init__(
        self,
        num: int,
        name: str,
        type: str | None,
        array: str | None,
        scale: list[int | str] | None,
        offset: list[int | str] | None,
        units: list[str] | None,
        bits: list[int | str] | None,
        components: list[int | str] | None,
        is_accumulated: bool,
        has_components: bool,
        message_num: int | None = None,
    ):
        self.num = int(num)
        self.name = str(name)
        self.type = type
        self.is_array = array == "true"
        self.scale = build_int_array_object(scale)
        self.offset = build_int_array_object(offset)
        self.units = build_str_array_object(units)
        self.bits = build_int_array_object(bits)
        self.components = build_int_array_object(components)
        self.is_accumulated = is_accumulated
        self.has_components = has_components
        self.message_num = message_num


class MessageFields(Columnar):
    __slots__ = MessageField.__slots__
    __annotations__ = listify_annotations(MessageField)


class MessageSubfield:
    name: str
    type: str | None
    is_array: bool
    scale: list[int | None] | None
    offset: list[int | None] | None
    units: list[str | None] | None
    bits: list[int | None] | None
    components: list[int | None] | None
    has_components: bool
    message_field_num: int | None
    message_field_name: str | None
    message_num: int | None
    __slots__ = tuple(__annotations__.keys())

    def __init__(
        self,
        name: str,
        type: str | None,
        array: str | None,
        scale: list[int | str] | None,
        offset: list[int | str] | None,
        units: list[str] | None,
        bits: list[int | str] | None,
        components: list[int | str] | None,
        has_components: bool,
        message_field_num: int | None = None,
        message_field_name: str | None = None,
        message_num: int | None = None,
    ):
        self.name = str(name)
        self.type = type
        self.is_array = array == "true"
        self.scale = build_int_array_object(scale)
        self.offset = build_int_array_object(offset)
        self.units = build_str_array_object(units)
        self.bits = build_int_array_object(bits)
        self.components = build_int_array_object(components)
        self.has_components = has_components
        self.message_num = message_num


class MessageSubfields(Columnar):
    __slots__ = MessageSubfield.__slots__
    __annotations__ = listify_annotations(MessageSubfield)


class MessageSubfieldMap:
    num: int
    name: str
    raw_value: int
    value_name: str
    message_subfield_name: str | None
    message_num: int | None
    __slots__ = tuple(__annotations__.keys())

    def __init__(
        self,
        num: int,
        name: str,
        raw_value: int,
        value_name: str,
        message_subfield_name: str | None = None,
        message_num: int | None = None,
    ):
        self.num = int(num)
        self.name = str(name)
        self.raw_value = int(raw_value)
        self.value_name = str(value_name)
        self.message_subfield_name = message_subfield_name
        self.message_num = message_num


class MessageSubfieldMaps(Columnar):
    __slots__ = MessageSubfieldMap.__slots__
    __annotations__ = listify_annotations(MessageSubfieldMap)


class MessageNum:
    message_name: str
    num: int
    __slots__ = tuple(__annotations__.keys())

    def __init__(self, message_name: str, num: int) -> None:
        self.message_name = message_name.lower()
        self.num = num


class MessageNums(Columnar):
    __slots__ = MessageNum.__slots__
    __annotations__ = listify_annotations(MessageNum)


class FitTypeProperty:
    fit_type: str
    num: int | None
    hex_num: str | None
    name: str
    __slots__ = tuple(__annotations__.keys())

    def __init__(self, fit_type: str, num: str, name: str) -> None:
        self.fit_type = fit_type
        self.num: int | None = int(num) if "x" not in num else None
        self.hex_num: str | None = num if "x" in num else None
        self.name = name


class FitTypeProperties(Columnar):
    __slots__ = FitTypeProperty.__slots__
    __annotations__ = listify_annotations(FitTypeProperty)


class ProfileVersionTypedDict(TypedDict):
    major: int
    minor: int
    patch: int
    type: str


class Version:
    major: int
    minor: int
    patch: int
    type: str
    str: str
    __slots__ = tuple(__annotations__.keys())

    def __init__(self, version_dict: ProfileVersionTypedDict) -> None:
        self.major = version_dict["major"]
        self.minor = version_dict["minor"]
        self.patch = version_dict["patch"]
        self.type = version_dict["type"]
        self.str = f"{self.major}_{self.minor}_{self.patch}"
