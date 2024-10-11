from typing import Any, Dict, Protocol


class IDataValidator(Protocol):
    def validate_columns(self, data: Dict[str, Any]) -> bool:
        """
        Ensure all required columns are present in the data.
        """

    def validate_field_lengths(
        self, data: Dict[str, Any], field_lengths: Dict[str, int]
    ) -> bool:
        """
        Check if the lengths of string fields in the data match the defined lengths.
        """
