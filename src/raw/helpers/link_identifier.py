from typing import Literal

from api_helpers.helpers.logging_config import I


class LinkIdentifier:
    def __init__(self, source: Literal["rp", "tf"], mapping: dict[str, str]) -> None:
        self.source = source
        self.mapping = mapping

    def identify_link(self, link: str) -> bool:
        if self.source == "rp":
            return self._identify_rp_link(link)
        elif self.source == "tf":
            return self._identify_tf_link(link)

    def _identify_rp_link(self, link: str) -> bool:
        return self._check_url_link(link, course_part_index=-3, course_id_index=-4)

    def _identify_tf_link(self, link: str) -> bool:
        return self._check_url_link(link, course_part_index=-5, course_id_index=-2)

    def _check_url_link(
        self, link: str, course_part_index: int, course_id_index: int
    ) -> bool:
        link_parts = link.split("/")
        course = link_parts[course_part_index]
        course_id = link_parts[course_id_index]
        try:
            mapped_id = self.mapping[course]
        except KeyError:
            I(f"Course {course} not in mapping")
            return False
        I(f"Course ID: {course_id} Mapped ID: {mapped_id}")
        return course_id == mapped_id
