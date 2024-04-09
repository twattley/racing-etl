from dataclasses import dataclass
from datetime import datetime


@dataclass
class RacingPostDataModel:
    race_timestamp: datetime
    race_date: str
    course_name: str
    race_class: str
    horse_name: str
    horse_type: str
    horse_age: str
    headgear: str
    conditions: str
    horse_price: str
    race_title: str
    distance: str
    distance_full: str
    going: str
    number_of_runners: str
    total_prize_money: int
    first_place_prize_money: int
    winning_time: str
    official_rating: str
    horse_weight: str
    draw: str
    country: str
    surface: str
    finishing_position: str
    total_distance_beaten: str
    ts_value: str
    rpr_value: str
    extra_weight: float
    comment: str
    race_time: str
    currency: str
    course: str
    jockey_name: str
    jockey_claim: str
    trainer_name: str
    sire_name: str
    dam_name: str
    dams_sire: str
    owner_name: str
    horse_id: str
    trainer_id: str
    jockey_id: str
    sire_id: str
    dam_id: str
    dams_sire_id: str
    owner_id: str
    race_id: str
    course_id: str
    meeting_id: str
    unique_id: str
    debug_link: str
    created_at: datetime
