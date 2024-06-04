import pandas as pd

from src.storage.psql_db import get_db

db = get_db()
import time

first_time = time.time()

# today = db.fetch_data(
#         """
#         SELECT pd.*, 'today'::character varying AS data_type
#         FROM public.unioned_performance_data pd
#         WHERE pd.race_date = '2023-01-01'::date
#         """
#     )
# today.to_parquet("today.parquet")
today_parquet = pd.read_parquet("today.parquet")

todays_time = time.time() - first_time
print(f"Time taken: {todays_time}")
# hist, col = ptr(

#     lambda: db.fetch_data(
#         """
#         SELECT pd.*, 'historical'::character varying AS data_type
#         FROM public.unioned_performance_data pd
#         WHERE pd.horse_id IN (
#             SELECT pd.horse_id
#             FROM public.unioned_performance_data pd
#             WHERE pd.race_date = '2023-01-01'::date
#         )
#         AND pd.race_date < '2023-01-01'::date
#         AND pd.race_date >= '2023-01-01'::date - INTERVAL '3 YEARS'
#         """
#     ),
#     lambda: db.fetch_data(
#         """
#         SELECT pd.*, 'collateral'::character varying AS data_type
#         FROM public.unioned_performance_data pd
#         WHERE pd.horse_id IN (
#             SELECT DISTINCT pd.horse_id
#             FROM public.unioned_performance_data pd
#             WHERE pd.race_date < '2023-01-01'::date
#             AND pd.race_date >= '2023-01-01'::date - INTERVAL '3 YEARS'
#             AND pd.race_id IN (
#                 SELECT DISTINCT pd.race_id
#                 FROM public.unioned_performance_data pd
#                 WHERE pd.horse_id IN (
#                     SELECT pd.horse_id
#                     FROM public.unioned_performance_data pd
#                     WHERE pd.race_date = '2023-01-01'::date
#                 )
#                 AND pd.race_date < '2023-01-01'::date
#                 AND pd.race_date >= '2023-01-01'::date - INTERVAL '3 YEARS'
#             )
#         )
#         AND pd.race_date < '2023-01-01'::date
#         AND pd.race_date >= '2023-01-01'::date - INTERVAL '3 YEARS';
#         """
#     ),
# )
# hist.to_parquet("hist.parquet")
# col.to_parquet("col.parquet")

hist_parquet = pd.read_parquet("hist.parquet")

col_parquet = pd.read_parquet("col.parquet")


# df = select_function("public", "select_race_date", ["'2023-01-01'"])
df = pd.concat([today_parquet, hist_parquet, col_parquet])
print(df.head())


completed_time = time.time() - first_time
print(f"Time taken: {completed_time}")
