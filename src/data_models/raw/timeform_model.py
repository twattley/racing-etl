from typing import Any, Dict

from src.data_models.interfaces.data_model_interface import IDataModel
from src.data_models.interfaces.data_validator_interface import IDataValidator


class TimeformDataModel(IDataModel):
    def __init__(self, validator: IDataValidator):
        self.validator = validator
        self.fields = [
            "horse_name",
            "horse_id",
            "horse_name_link",
            "horse_age",
            "equipment",
            "official_rating",
            "fractional_price",
            "betfair_win_sp",
            "betfair_place_sp",
            "in_play_prices",
            "tf_comment",
            "course",
            "race_date",
            "race_time",
            "race_timestamp",
            "course_id",
            "race",
            "race_id",
            "distance",
            "going",
            "prize",
            "hcap_range",
            "age_range",
            "race_type",
            "main_race_comment",
            "debug_link",
            "created_at",
            "unique_id",
        ]

        self.field_lengths = {
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

    def validate_columns(self, data: Dict[str, Any]) -> bool:
        return self.validator.validate_columns(data, self.fields)

    def validate_field_lengths(
        self, data: Dict[str, Any], field_lengths: Dict[str, int]
    ) -> bool:
        return self.validator.validate_field_lengths(data, field_lengths)
