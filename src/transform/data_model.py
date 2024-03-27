
from dataclasses import dataclass, fields
from datetime import date, datetime
import pandas as pd

@dataclass
class BaseDataModel():
    pass

@dataclass
class TransformedDataModel(BaseDataModel):
    race_time: datetime
    race_date: date
    horse_name: str
    age: int
    draw: int
    headgear: str
    weight_carried_st_lbs: str
    weight_carried_lbs: int
    extra_weight_lbs: int
    jockey_claim: str
    finishing_position: str
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
    tf_rating_view: str
    course_id: int
    horse_id: int
    jockey_id: int
    trainer_id: int
    owner_id: int
    sire_id: int
    dam_id: int
    unique_id: str
    created_at: datetime


@dataclass
class RaceDataModel(BaseDataModel):
    race_time: datetime
    race_date: date
    race_title: str
    race_type: str
    distance: str
    yards: float
    meters: float
    kilometers: float
    conditions: str
    going: str
    number_of_runners: int
    hcap_range: str
    age_range: str
    surface: str
    prize: str
    total_prize_money: int
    first_place_prize_money: int
    winning_time: str
    time_seconds: float
    relative_time: float
    relative: str
    course_name: str
    country: str
    main_race_comment: str
    meeting_id: str
    race_id: str
    course_id: int
    created_at: datetime





def convert_data(
    data: pd.DataFrame, data_model: BaseDataModel, 
) -> pd.DataFrame:

    for field in fields(data_model):
        if field.type == datetime:
            data[field.name] = pd.to_datetime(data[field.name], errors="coerce")
        if field.type == date:
            data[field.name] = pd.to_datetime(data[field.name], errors="coerce").dt.date
        elif field.type == int:
            data[field.name] = pd.to_numeric(data[field.name], errors="coerce").astype(
                "Int64"
            )
        elif field.type == float:
            data[field.name] = pd.to_numeric(data[field.name], errors="coerce")
    return data
