from typing import Dict, List, Union

import pandas as pd


def assert_data_equal(received_data, expected_data, comparison_keys: List[str] = None):
    if comparison_keys:
        assert {
            horse: [
                {k: v for k, v in race.items() if k in comparison_keys}
                for race in races
            ]
            for horse, races in received_data.items()
        } == expected_data
    else:
        assert received_data == expected_data


def assert_df_data_equal(
    received_df: pd.DataFrame,
    expected_df: pd.DataFrame,
    test_columns: List[str] = None,
    **kwargs,
):


    print("[ Tests ] - Received data:")

    if test_columns:
        print(received_df[test_columns])
    else:
        print(received_df)
    print("[ Tests ] - Expected data:")
    if test_columns:
        print(expected_df[test_columns])
    else:
        print(expected_df)

    pd.testing.assert_frame_equal(
        received_df.sort_index(axis=1).reset_index(drop=True),
        expected_df.sort_index(axis=1).reset_index(drop=True),
        check_exact=True,
        **kwargs,
    )


def merge_data(dict_1, dict_2):
    return {**dict_1, **dict_2}


def print_dataframe_for_testing(df):

    print("pd.DataFrame({")

    for col in df.columns:
        print(f"'{col}':{list(df[col])},")
    print("})")


class FakeBetfairClient:
    """
    Test Wrapper around betfair client
    """

    def __init__(self):
        self.placed_orders = []

    def set_current_orders(self, current_orders: pd.DataFrame) -> None:
        self.current_orders = current_orders

    def place_orders(self, size, price, selection_id, market_id, side):
        print(
            f"Placing order: size: {size}, price: {price}, selection_id: {selection_id}, market_id: {market_id}, side: {side}"
        )
        self.placed_orders.append(
            {
                "size": size,
                "price": price,
                "selection_id": selection_id,
                "market_id": market_id,
                "side": side,
            }
        )

    def cancel_orders(self, market_ids):
        self.market_ids = market_ids

    def get_current_orders(self):
        return self.current_orders

    def get_balance(self):
        return 1000
