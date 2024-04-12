from dataclasses import dataclass
from datetime import datetime


@dataclass
class TimeformDataModel:
    tf_rating: str
    tf_speed_figure: str
    draw: str
    trainer_name: str
    trainer_id: str
    jockey_name: str
    jockey_id: str
    sire_name: str
    sire_id: str
    dam_name: str
    dam_id: str
    finishing_position: str
    horse_name: str
    horse_id: str
    horse_name_link: str
    horse_age: str
    equipment: str
    official_rating: str
    fractional_price: str
    betfair_win_sp: str
    betfair_place_sp: str
    in_play_prices: str
    tf_comment: str
    course: str
    race_date: str
    race_time: str
    race_timestamp: datetime
    course_id: str
    race: str
    race_id: str
    distance: str
    going: str
    prize: str
    hcap_range: str
    age_range: str
    race_type: str
    main_race_comment: str
    debug_link: str
    created_at: datetime
    unique_id: str


table_string_field_lengths = {
    "tf_rating": 16,
    "tf_speed_figure": 16,
    "draw": 16,
    "trainer_name": 132,
    "trainer_id": 32,
    "jockey_name": 132,
    "jockey_id": 32,
    "sire_name": 132,
    "sire_id": 32,
    "dam_name": 132,
    "dam_id": 32,
    "finishing_position": 16,
    "horse_name": 132,
    "horse_id": 32,
    "horse_name_link": 132,
    "horse_age": 16,
    "equipment": 16,
    "official_rating": 16,
    "fractional_price": 64,
    "betfair_win_sp": 16,
    "betfair_place_sp": 16,
    "in_play_prices": 16,
    "race_date": 32,
    "race_time": 32,
    "course_id": 32,
    "race_id": 132,
    "distance": 32,
    "going": 16,
    "prize": 16,
    "hcap_range": 16,
    "age_range": 16,
    "race_type": 16,
    "unique_id": 132,
}
