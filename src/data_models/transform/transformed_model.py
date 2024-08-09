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
    total_distance_beaten: float
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
    race_id: int
    course_id: int
    horse_id: int
    jockey_id: int
    trainer_id: int
    owner_id: int
    sire_id: int
    dam_id: int
    unique_id: str
    created_at: datetime


table_string_field_lengths = {
    "horse_name": 132,
    "horse_sex": 32,
    "headgear": 64,
    "weight_carried": 16,
    "finishing_position": 6,
    "industry_sp": 16,
    "tfr_view": 16,
    "unique_id": 132,
}

distance_map = {
    "1m": 1760,
    "1m½f": 1870,
    "1m1½f": 2090,
    "1m1f": 1980,
    "1m2½f": 2310,
    "1m2f": 2200,
    "1m3½f": 2530,
    "1m3f": 2420,
    "1m4½f": 2750,
    "1m4f": 2640,
    "1m5½f": 2970,
    "1m5f": 2860,
    "1m6½f": 3190,
    "1m6f": 3080,
    "1m7½f": 3410,
    "1m7f": 3300,
    "2m": 3520,
    "2m½f": 3630,
    "2m1½f": 3850,
    "2m1f": 3740,
    "2m2½f": 4070,
    "2m2f": 3960,
    "2m3½f": 4290,
    "2m3f": 4180,
    "2m4½f": 4510,
    "2m4f": 4400,
    "2m5½f": 4730,
    "2m5f": 4620,
    "2m6½f": 4950,
    "2m6f": 4840,
    "2m7½f": 5170,
    "2m7f": 5060,
    "3m": 5280,
    "3m½f": 5390,
    "3m1½f": 5610,
    "3m1f": 5500,
    "3m2½f": 5830,
    "3m2f": 5720,
    "3m3½f": 6150,
    "3m3f": 5940,
    "3m4½f": 6270,
    "3m4f": 6160,
    "3m5½f": 6490,
    "3m5f": 6380,
    "3m6½f": 6710,
    "3m6f": 6600,
    "3m7½f": 6930,
    "3m7f": 6820,
    "4m": 7040,
    "4m½f": 7150,
    "4m1½f": 7370,
    "4m1f": 7260,
    "4m2½f": 7590,
    "4m2f": 7480,
    "4m3f": 7700,
    "4m4f": 7920,
    "5½f": 1210,
    "5f": 1100,
    "6½f": 1430,
    "6f": 1320,
    "7½f": 1650,
    "7f": 1540,
}
