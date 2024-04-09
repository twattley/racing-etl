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
