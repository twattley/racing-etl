from dataclasses import dataclass, fields
from datetime import date, datetime

import pandas as pd

from src.utils.logging_config import I, W


@dataclass
class BaseDataModel: ...


def convert_data(
    data: pd.DataFrame,
    data_model: BaseDataModel,
) -> pd.DataFrame:

    I("Converting data...")

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

    I("Data converted successfully")

    return data


def validate_data(
    data: pd.DataFrame,
    data_model: BaseDataModel,
) -> pd.DataFrame:

    I("Validating data...")

    data_model_fields = {field.name for field in fields(data_model)}
    missing_columns = ", ".join(data_model_fields - set(data.columns))
    if missing_columns:
        raise ValueError(
            f"Data does not contain all required columns, missing: {missing_columns}"
        )

    extra_columns = ", ".join(set(data.columns) - data_model_fields)
    if extra_columns:
        raise ValueError(f"Data contains extra columns: {extra_columns}")

    if sorted(data.columns) != sorted([field.name for field in fields(data_model)]):
        raise ValueError("Data missing columns or has extra columns.")

    I("Data validated successfully")

    return data


def sort_data(
    data: pd.DataFrame, data_model: BaseDataModel, unique_id: str
) -> pd.DataFrame:
    I("Sorting data...")
    I(f"Data Columns {data.columns}")
    I(f"Data Sample {data.head(10)}")

    data = data.sort_values(by="created_at", ascending=False).drop_duplicates(
        subset=[unique_id]
    )[[field.name for field in fields(data_model)]]
    I(f"Data Sample {data.head(10)}")
    return data


def check_string_field_lengths(
    data: pd.DataFrame, string_lengths: dict
) -> pd.DataFrame:
    values_too_long = []
    for field, value_ in string_lengths.items():
        if field in data.columns:
            subset_df = data[[field]].copy()
            subset_df[f"truncated_{field}"] = subset_df[field].str[
                : string_lengths[field]
            ]
            subset_df["miss_trunc"] = (
                subset_df[field] != subset_df[f"truncated_{field}"]
            )
            subset_df = subset_df[subset_df["miss_trunc"] == True].dropna(
                subset=[field]
            )
            if subset_df.empty:
                continue
            print(subset_df)
            values_too_long.extend(
                [field, row[field], len(row[field]), value_]
                for _, row in subset_df.iterrows()
            )
    if values_too_long:
        for value in values_too_long:
            W(
                f"Value of {value[0]} that is too long: {value[1]}, string length: {value[2]} > {value[3]}"
            )
        raise ValueError("Failed value length check")
    I("Data string lengths checked successfully")
    return data


def convert_and_validate_data(
    data: pd.DataFrame,
    data_model: BaseDataModel,
    string_lengths: dict,
    unique_id: str,
) -> pd.DataFrame:

    return (
        data.pipe(check_string_field_lengths, string_lengths)
        .pipe(convert_data, data_model)
        .pipe(sort_data, data_model, unique_id)
        .pipe(validate_data, data_model)
        .pipe(sort_data, data_model, unique_id)
    )
