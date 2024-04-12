from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class RaceDataModel:
    race_time: datetime
    race_date: date
    race_title: str
    race_type: str
    race_class: int
    distance: str
    distance_yards: float
    distance_meters: float
    distance_kilometers: float
    conditions: str
    going: str
    number_of_runners: int
    hcap_range: str
    age_range: str
    surface: str
    total_prize_money: int
    first_place_prize_money: int
    winning_time: str
    time_seconds: float
    relative_time: float
    relative_to_standard: str
    country: str
    main_race_comment: str
    meeting_id: str
    race_id: str
    course_id: int
    created_at: datetime


table_string_field_lengths = {
    "race_title": 132,
    "race_type": 32,
    "distance": 16,
    "conditions": 32,
    "going": 32,
    "hcap_range": 32,
    "age_range": 32,
    "surface": 32,
    "winning_time": 32,
    "relative_to_standard": 16,
    "country": 64,
    "meeting_id": 132,
    "race_id": 132,
}
