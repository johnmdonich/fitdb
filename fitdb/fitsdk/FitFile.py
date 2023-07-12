import datetime
from hashlib import blake2s
from pathlib import Path
from typing import Any, Callable

from garmin_fit_sdk import Decoder, Stream

from .Columnar import Columnar, listify_annotations

FILE_HASH_SIZE = 16


class FitFileMeasure:
    fit_file_hash: str
    timestamp: datetime.datetime
    measure_type: str
    measure_value: float | None
    __slots__ = tuple(__annotations__.keys())

    def __init__(
        self,
        fit_file_hash: str,
        timestamp: datetime.datetime,
        measure_type: str,
        measure_value: float | None,
    ) -> None:
        self.fit_file_hash = fit_file_hash
        self.timestamp = timestamp
        self.measure_type = measure_type
        if not isinstance(measure_value, (float, int)):
            self.measure_value = None
        else:
            self.measure_value = measure_value

class FitFileMeasures(Columnar):
    __slots__ = FitFileMeasure.__slots__
    __annotations__ = listify_annotations(FitFileMeasure)

class FitFile:
    _stream: Stream
    _decoder: Decoder
    filename: Path | str
    fit_file_hash: str
    measure_types: tuple[str, ...]
    messages: dict[str, Any]
    errors: dict[str, Any]
    
    def __init__(self, filename: str | Path):
        self.filename = filename
        self._stream = Stream.from_file(self.filename)
        self._decoder = Decoder(self._stream)
        self.messages, self.errors = self.load()
        self.fit_file_hash = self.file_hash()
        self.measure_types = self.record_keys()
        self.measures = self.parse_measurements()
    
    def load(self, apply_scale_and_offset: bool=True,
            convert_datetimes_to_dates: bool=True,
            convert_types_to_strings: bool=True,
            expand_sub_fields: bool=True,
            expand_components: bool=True,
            merge_heart_rates: bool=True,
            mesg_listener: Callable[[int, str], None] | None = None,) -> tuple[dict[str, Any], dict[str, Any]]:
        return self._decoder.read(
            apply_scale_and_offset=apply_scale_and_offset,
            convert_datetimes_to_dates=convert_datetimes_to_dates,
            convert_types_to_strings=convert_types_to_strings,
            expand_sub_fields=expand_sub_fields,
            expand_components=expand_components,
            merge_heart_rates=merge_heart_rates,
            mesg_listener=mesg_listener,
        )
    
    def file_hash(self) -> str:
        hasher = blake2s(digest_size=FILE_HASH_SIZE)
        hash_str = f"{self.messages['file_id_mesgs'][0]}"
        hasher.update(bytes(hash_str, encoding='utf-8'))
        return hasher.hexdigest()
    
    def record_keys(self) -> tuple[str, ...]:
        record_keys_set = self.messages["record_mesgs"][0].keys()
        for record in self.messages["record_mesgs"][1:100]:
            record_keys_set = record_keys_set | record.keys()
        return tuple(record_keys_set)
    
    def parse_measurements(self) -> FitFileMeasures:
        fit_file_measures = FitFileMeasures()
        for record in self.messages["record_mesgs"]:
            timestamp = record["timestamp"]
            for measure_type, measure_value in record.items():
                if measure_type != "timestamp":
                    fit_file_measures.append(
                        FitFileMeasure(
                            fit_file_hash=self.fit_file_hash,
                            timestamp=timestamp,
                            measure_type=measure_type,
                            measure_value=measure_value,
                        )
                    )
        return fit_file_measures
    