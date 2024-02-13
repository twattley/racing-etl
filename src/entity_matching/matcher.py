import pandas as pd


# Define the Levenshtein distance function
def levenshtein_distance(s1: str, s2: str) -> int:
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if not s2:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def calculate_similarity(
    df: pd.DataFrame, column1: str, column2: str, new_column_name: str
) -> pd.DataFrame:
    df[new_column_name] = df.apply(
        lambda row: 1
        - levenshtein_distance(row[column1], row[column2])
        / max(len(row[column1]), len(row[column2])),
        axis=1,
    )
    return df


def join_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, column1: str, column2: str
) -> pd.DataFrame:
    return df1.merge(df2, how="inner", left_on=column1, right_on=column2)
