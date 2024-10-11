from typing import Any, Dict

from src.data_models.interfaces.data_validator_interface import IDataValidator


class DataValidator(IDataValidator):
    def __init__(self, validator: IDataValidator):
        self.validator = validator

    def validate_columns(self, data: Dict[str, Any]) -> bool:
        self.validator.validate_columns(data, self.fields)
        missing_fields = set(self.fields) - set(data.keys())
        if missing_fields:
            raise ValueError(f"Missing fields: {', '.join(missing_fields)}")
        return True

    def validate_field_lengths(
        self, data: Dict[str, Any], field_lengths: Dict[str, int]
    ) -> bool:
        self.validator.validate_field_lengths(data, field_lengths)
        for field, max_length in field_lengths.items():
            if field in data and isinstance(data[field], str):
                if len(data[field]) > max_length:
                    raise ValueError(
                        f"Field '{field}' exceeds maximum length of {max_length}"
                    )
        return True
