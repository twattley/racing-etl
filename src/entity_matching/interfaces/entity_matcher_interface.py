from typing import Protocol

import pandas as pd


class IEntityMatcher(Protocol):
    def match_data(self, *args, **kwargs) -> pd.DataFrame: ...
