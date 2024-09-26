from dataclasses import dataclass


@dataclass
class RawError:
    source: str
    error_url: str
    error_message: str
