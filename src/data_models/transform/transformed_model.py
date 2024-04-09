from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class TransformedDataModel:
    race_time: datetime
    race_date: date
    horse_name: str
    age: int
    horse_sex: str
    draw: int
    headgear: str
    weight_carried: str
    weight_carried_lbs: int
    extra_weight: int
    jockey_claim: str
    finishing_position: str
    total_distance_beaten: str
    industry_sp: str
    betfair_win_sp: float
    betfair_place_sp: float
    official_rating: int
    ts: int
    rpr: int
    tfig: int
    tfr: int
    in_play_high: float
    in_play_low: float
    in_race_comment: str
    tf_comment: str
    tfr_view: str
    course_id: int
    horse_id: int
    jockey_id: int
    trainer_id: int
    owner_id: int
    sire_id: int
    dam_id: int
    unique_id: str
    created_at: datetime
