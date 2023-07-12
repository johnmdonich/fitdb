from typing import Any, Generator, Optional

from garmin_fit_sdk import Profile

from .ProfileTypes import (
    FitTypeProperty,
    FitTypeProperties,
    Message,
    Messages,
    MessageField,
    MessageFields,
    MessageNum,
    MessageNums,
    MessageSubfield,
    MessageSubfields,
    MessageSubfieldMap,
    MessageSubfieldMaps,
    Version,
)


MessageType = Message | MessageField | MessageSubfield | MessageSubfieldMap


class FitDBProfile:
    messages: Messages
    message_fields: MessageFields
    message_subfields: MessageSubfields
    message_subfield_maps: MessageSubfieldMaps
    message_nums: MessageNums
    fit_type_properties: FitTypeProperties
    version: Version | None
    __slots__ = tuple(__annotations__.keys())

    def __init__(self) -> None:
        self.messages = Messages()
        self.message_fields = MessageFields()
        self.message_subfields = MessageSubfields()
        self.message_subfield_maps = MessageSubfieldMaps()
        self.message_nums = MessageNums()
        self.fit_type_properties = FitTypeProperties()
        self.version = None
        self.parse_messages()

    @staticmethod
    def message_generator(
        MessageType: type[MessageType],
        profile_dict: dict[str, Any],
        nested_dict_key: Optional[str] = None,
    ) -> Generator[tuple[Any, Optional[dict[str, Any]]], None, None]:
        init_keys = MessageType.__init__.__annotations__.keys()
        if isinstance(profile_dict, list):
            profile_dict = {idx: value for idx, value in enumerate(profile_dict)}

        for idx, dictionary in profile_dict.items():
            class_dict = {
                key: val for key, val in dictionary.items() if key in init_keys
            }
            message_instance = MessageType(**class_dict)
            nested_dict = dictionary.get(nested_dict_key)
            yield message_instance, nested_dict

    def parse_messages(self) -> None:
        self.version: Version = Version(Profile["version"])

        for message, fields_dict in self.message_generator(
            Message, Profile["messages"], "fields"
        ):
            self.messages.append(message)
            assert fields_dict is not None
            for message_field, subfield_dict in self.message_generator(
                MessageField, fields_dict, "sub_fields"
            ):
                message_field.message_num = message.num
                self.message_fields.append(message_field)

                assert subfield_dict is not None
                for message_sub_field, subfield_map_dict in self.message_generator(
                    MessageSubfield, subfield_dict, "map"
                ):
                    message_sub_field.message_field_num = message_field.num
                    message_sub_field.message_field_name = message_field.name
                    message_sub_field.message_num = message.num
                    self.message_subfields.append(message_sub_field)

                    assert subfield_map_dict is not None
                    for message_sub_field_map, nothing in self.message_generator(
                        MessageSubfieldMap, subfield_map_dict, None
                    ):
                        message_sub_field_map.message_subfield_name = (
                            message_sub_field.name
                        )
                        message_sub_field_map.message_num = message.num
                        self.message_subfield_maps.append(message_sub_field_map)

        for message_name, num in Profile["mesg_num"].items():
            self.message_nums.append(MessageNum(message_name, num))

        for fit_type, fit_type_dict in Profile["types"].items():
            for id, name in fit_type_dict.items():
                self.fit_type_properties.append(FitTypeProperty(fit_type, id, name))
