from typing import Protocol


class ILinkIdentifier(Protocol):
    def identify_link(self, link: str) -> bool: ...
