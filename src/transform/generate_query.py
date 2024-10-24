from typing import Literal


class TransformSQLGenerator:
    @staticmethod
    def define_results_data_join_sql(
        data_table: str, time_period: str, data_type: Literal["historical", "today"]
    ):
        return f"""
            SELECT 
            rp.horse_name as horse_name,
            rp.horse_age as horse_age,
            rp.jockey_name as jockey,
            rp.jockey_claim as jockey_claim,
            rp.trainer_name as trainer,
            rp.owner_name as owner,
            rp.horse_weight as weight_carried,
            rp.draw,
            COALESCE(rp.finishing_position, tf.finishing_position) AS finishing_position,
            rp.total_distance_beaten,
            COALESCE(rp.horse_age, tf.horse_age) AS age,
            COALESCE(rp.official_rating, tf.official_rating) AS official_rating,
            rp.ts_value as ts,
            rp.rpr_value as rpr,
            rp.horse_price as industry_sp,
            rp.extra_weight as extra_weight,
            rp.headgear,
            rp.comment as in_race_comment,
            rp.sire_name as sire,
            rp.dam_name as dam,
            rp.race_date as race_date,
            rp.race_title as race_title,
            rp.race_time as race_time,
            rp.race_class as race_class,
            rp.conditions as conditions,
            rp.distance as distance,
            rp.distance_full as distance_full,
            rp.going as going,
            rp.winning_time as winning_time,
            rp.horse_type as horse_type,
            rp.number_of_runners as number_of_runners,
            rp.total_prize_money as total_prize_money,
            rp.first_place_prize_money,
            rp.currency,
            rp.course_name as course,
            rp.country as country,
            rp.surface as surface,
            tf.tf_rating as tfr,
            tf.tf_speed_figure as tfig,
            tf.betfair_win_sp as betfair_win_sp,
            tf.betfair_place_sp as betfair_place_sp,
            tf.in_play_prices as in_play_prices,
            tf.tf_comment as tf_comment,
            tf.hcap_range as hcap_range,
            tf.age_range as age_range,
            tf.race_type as race_type,
            tf.main_race_comment as main_race_comment,
            rp.meeting_id as meeting_id,
            c.id as course_id,
            h.id as horse_id,
            s.id as sire_id,
            d.id as dam_id,
            t.id as trainer_id,
            j.id as jockey_id,
            o.id as owner_id,
            rp.race_id as race_id,
            rp.unique_id as unique_id,
            ROW_NUMBER() OVER(PARTITION BY rp.unique_id ORDER BY rp.created_at) AS rn,
            rp.debug_link as debug_link,
            rp.adj_total_distance_beaten,
            '{data_type}' as data_type
        FROM rp_raw.{data_table} rp
        LEFT JOIN results_data pr ON rp.unique_id::text = pr.unique_id::text
        LEFT JOIN entities.course c ON rp.course_id = c.rp_id
        LEFT JOIN entities.horse h ON rp.horse_id = h.rp_id
        LEFT JOIN entities.sire s ON rp.sire_id = s.rp_id
        LEFT JOIN entities.dam d ON rp.dam_id = d.rp_id
        LEFT JOIN entities.trainer t ON rp.trainer_id = t.rp_id
        LEFT JOIN entities.jockey j ON rp.jockey_id = j.rp_id
        LEFT JOIN entities.owner o ON rp.owner_id = o.rp_id
        LEFT JOIN tf_raw.{data_table} tf ON tf.horse_id = h.tf_id
            AND tf.course_id = c.tf_id
            AND tf.race_date = rp.race_date
        WHERE pr.unique_id IS NULL 
        AND rp.unique_id IS NOT NULL 
        AND rp.race_time > (now() - '{time_period}'::interval)
        AND tf.unique_id IS NOT NULL 
    """

    @staticmethod
    def define_results_data_world_join_sql(
        data_table: str, time_period: str, data_type: Literal["historical", "today"]
    ):
        return f"""
        SELECT 
            rp.horse_name as horse_name,
            rp.horse_age as horse_age,
            rp.jockey_name as jockey,
            rp.jockey_claim as jockey_claim,
            rp.trainer_name as trainer,
            rp.owner_name as owner,
            rp.horse_weight as weight_carried,
            rp.draw,
            COALESCE(rp.finishing_position, tf.finishing_position) AS finishing_position,
            rp.total_distance_beaten,
            COALESCE(rp.horse_age, tf.horse_age) AS age,
            COALESCE(rp.official_rating, tf.official_rating) AS official_rating,
            rp.ts_value as ts,
            rp.rpr_value as rpr,
            rp.horse_price as industry_sp,
            rp.extra_weight as extra_weight,
            rp.headgear,
            rp.comment as in_race_comment,
            rp.sire_name as sire,
            rp.dam_name as dam,
            rp.race_date as race_date,
            rp.race_title as race_title,
            rp.race_time as race_time,
            rp.race_class as race_class,
            rp.conditions as conditions,
            rp.distance as distance,
            rp.distance_full as distance_full,
            rp.going as going,
            rp.winning_time as winning_time,
            rp.horse_type as horse_type,
            rp.number_of_runners as number_of_runners,
            rp.total_prize_money as total_prize_money,
            rp.first_place_prize_money,
            rp.currency,
            rp.course_name as course,
            rp.country as country,
            COALESCE(
                (regexp_match(rp.race_title, '\(([^)]+)\)\s*$'))[1],
                'Unknown'
            ) AS surface,
            CASE 
                WHEN tf.tf_rating IS NOT NULL AND tf.tf_rating != '' THEN tf.tf_rating
                WHEN rp.rpr_value IS NOT NULL AND rp.rpr_value != '' THEN rp.rpr_value
                ELSE NULL
            END AS tfr,
            
            CASE 
                WHEN tf.tf_speed_figure IS NOT NULL AND tf.tf_speed_figure != '' THEN tf.tf_speed_figure
                WHEN rp.ts_value IS NOT NULL AND rp.ts_value != '' THEN rp.ts_value
                ELSE NULL
            END AS tfig,
            tf.betfair_win_sp as betfair_win_sp,
            tf.betfair_place_sp as betfair_place_sp,
            tf.in_play_prices as in_play_prices,
            COALESCE(tf.tf_comment, rp.comment) AS tf_comment,
            tf.hcap_range as hcap_range,
            tf.age_range as age_range,
            tf.race_type as race_type,
            tf.main_race_comment as main_race_comment,
            rp.meeting_id as meeting_id,
            c.id as course_id,
            h.id as horse_id,
            s.id as sire_id,
            d.id as dam_id,
            t.id as trainer_id,
            j.id as jockey_id,
            o.id as owner_id,
            rp.race_id as race_id,
            rp.unique_id as unique_id,
            ROW_NUMBER() OVER(PARTITION BY rp.unique_id ORDER BY rp.created_at) AS rn,
            rp.debug_link as debug_link,
            rp.adj_total_distance_beaten,
            '{data_type}' as data_type
        FROM rp_raw.{data_table} rp
        LEFT JOIN results_data pr ON rp.unique_id::text = pr.unique_id::text
        LEFT JOIN entities.course c ON rp.course_id = c.rp_id
        LEFT JOIN entities.horse h ON rp.horse_id = h.rp_id
        LEFT JOIN entities.sire s ON rp.sire_id = s.rp_id
        LEFT JOIN entities.dam d ON rp.dam_id = d.rp_id
        LEFT JOIN entities.trainer t ON rp.trainer_id = t.rp_id
        LEFT JOIN entities.jockey j ON rp.jockey_id = j.rp_id
        LEFT JOIN entities.owner o ON rp.owner_id = o.rp_id
        LEFT JOIN tf_raw.{data_table} tf ON tf.horse_id = h.tf_id
            AND tf.course_id = c.tf_id
            AND tf.race_date = rp.race_date
        WHERE pr.unique_id IS NULL 
        AND rp.unique_id IS NOT NULL 
        AND rp.race_time > (now() - '{time_period}'::interval)
        AND tf.unique_id IS NOT NULL 
        AND h.rp_id IS NOT NULL
        """

    @staticmethod
    def get_joined_results_data_sql():
        return f"""
        WITH joined_data AS (
            {TransformSQLGenerator.define_results_data_join_sql('results_data', '5 years', 'historical')}
            UNION 
            {TransformSQLGenerator.define_results_data_world_join_sql('results_data_world', '9 years', 'historical')}
        )
        SELECT * FROM joined_data WHERE rn = 1
        """

    @staticmethod
    def get_joined_todays_data_sql():
        return """
        WITH joined_data AS (
            SELECT 
                    rp.horse_name as horse_name,
                    rp.horse_age as horse_age,
                    rp.jockey_name as jockey,
                    rp.jockey_claim as jockey_claim,
                    rp.trainer_name as trainer,
                    rp.owner_name as owner,
                    rp.horse_weight as weight_carried,
                    rp.draw,
                    COALESCE(rp.finishing_position, tf.finishing_position) AS finishing_position,
                    rp.total_distance_beaten,
                    COALESCE(rp.horse_age, tf.horse_age) AS age,
                    COALESCE(rp.official_rating, tf.official_rating) AS official_rating,
                    rp.ts_value as ts,
                    rp.rpr_value as rpr,
                    rp.horse_price as industry_sp,
                    rp.extra_weight as extra_weight,
                    rp.headgear,
                    rp.comment as in_race_comment,
                    rp.sire_name as sire,
                    rp.dam_name as dam,
                    rp.race_date as race_date,
                    rp.race_title as race_title,
                    rp.race_time as race_time,
                    rp.race_class as race_class,
                    rp.conditions as conditions,
                    rp.distance as distance,
                    rp.distance_full as distance_full,
                    rp.going as going,
                    rp.winning_time as winning_time,
                    rp.horse_type as horse_type,
                    rp.number_of_runners as number_of_runners,
                    rp.total_prize_money as total_prize_money,
                    rp.first_place_prize_money,
                    rp.currency,
                    rp.course_name as course,
                    rp.country as country,
                    rp.surface as surface,
                    tf.tf_rating as tfr,
                    tf.tf_speed_figure as tfig,
                    tf.betfair_win_sp as betfair_win_sp,
                    tf.betfair_place_sp as betfair_place_sp,
                    tf.in_play_prices as in_play_prices,
                    tf.tf_comment as tf_comment,
                    tf.hcap_range as hcap_range,
                    tf.age_range as age_range,
                    tf.race_type as race_type,
                    tf.main_race_comment as main_race_comment,
                    rp.meeting_id as meeting_id,
                    c.id as course_id,
                    h.id as horse_id,
                    s.id as sire_id,
                    d.id as dam_id,
                    t.id as trainer_id,
                    j.id as jockey_id,
                    o.id as owner_id,
                    rp.race_id as race_id,
                    rp.unique_id as unique_id,
                    ROW_NUMBER() OVER(PARTITION BY rp.unique_id ORDER BY rp.created_at) AS rn,
                    rp.debug_link as debug_link,
                    rp.adj_total_distance_beaten,
                    'today' as data_type
                FROM rp_raw.todays_data rp
                LEFT JOIN entities.course c ON rp.course_id = c.rp_id
                LEFT JOIN entities.horse h ON rp.horse_id = h.rp_id
                LEFT JOIN entities.sire s ON rp.sire_id = s.rp_id
                LEFT JOIN entities.dam d ON rp.dam_id = d.rp_id
                LEFT JOIN entities.trainer t ON rp.trainer_id = t.rp_id
                LEFT JOIN entities.jockey j ON rp.jockey_id = j.rp_id
                LEFT JOIN entities.owner o ON rp.owner_id = o.rp_id
                LEFT JOIN tf_raw.todays_data tf ON tf.horse_id = h.tf_id
                    AND tf.course_id = c.tf_id
                    AND tf.race_date = rp.race_date
                AND tf.unique_id IS NOT NULL 
            )
        SELECT * FROM joined_data WHERE rn = 1

        """


class ResultsDataSQLGenerator:
    @staticmethod
    def define_upsert_sql(
        table_name: str,
    ):
        return f"""
        INSERT INTO public.{table_name}(
            horse_name,
            age,
            horse_sex,
            draw,
            headgear,
            weight_carried,
            weight_carried_lbs,
            extra_weight,
            jockey_claim,
            finishing_position,
            total_distance_beaten,
            industry_sp,
            betfair_win_sp,
            betfair_place_sp,
            official_rating,
            ts,
            rpr,
            tfr,
            tfig,
            in_play_high,
            in_play_low,
            in_race_comment,
            tf_comment,
            tfr_view,
            race_id,
            horse_id,
            jockey_id,
            trainer_id,
            owner_id,
            sire_id,
            dam_id,
            unique_id,
            race_time,
            race_date,
            race_title,
            race_type,
            race_class,
            distance,
            distance_yards,
            distance_meters,
            distance_kilometers,
            conditions,
            going,
            number_of_runners,
            hcap_range,
            age_range,
            surface,
            total_prize_money,
            first_place_prize_money,
            winning_time,
            time_seconds,
            relative_time,
            relative_to_standard,
            country,
            main_race_comment,
            meeting_id,
            course_id,
            course,
            dam,
            sire,
            trainer,
            jockey,
            created_at
        )
        SELECT
            horse_name,
            age,
            horse_sex,
            draw,
            headgear,
            weight_carried,
            weight_carried_lbs,
            extra_weight,
            jockey_claim,
            finishing_position,
            total_distance_beaten,
            industry_sp,
            betfair_win_sp,
            betfair_place_sp,
            official_rating,
            ts,
            rpr,
            tfr,
            tfig,
            in_play_high,
            in_play_low,
            in_race_comment,
            tf_comment,
            tfr_view,
            race_id,
            horse_id,
            jockey_id,
            trainer_id,
            owner_id,
            sire_id,
            dam_id,
            unique_id,
            race_time,
            race_date,
            race_title,
            race_type,
            race_class,
            distance,
            distance_yards,
            distance_meters,
            distance_kilometers,
            conditions,
            going,
            number_of_runners,
            hcap_range,
            age_range,
            surface,
            total_prize_money,
            first_place_prize_money,
            winning_time,
            time_seconds,
            relative_time,
            relative_to_standard,
            country,
            main_race_comment,
            meeting_id,
            course_id,
            course,
            dam,
            sire,
            trainer,
            jockey,
            created_at
        FROM
            public_{table_name}_tmp_load
        ON CONFLICT(unique_id)
            DO UPDATE SET
                horse_name=EXCLUDED.horse_name,
                age=EXCLUDED.age,
                horse_sex=EXCLUDED.horse_sex,
                draw=EXCLUDED.draw,
                headgear=EXCLUDED.headgear,
                weight_carried=EXCLUDED.weight_carried,
                weight_carried_lbs=EXCLUDED.weight_carried_lbs,
                extra_weight=EXCLUDED.extra_weight,
                jockey_claim=EXCLUDED.jockey_claim,
                finishing_position=EXCLUDED.finishing_position,
                total_distance_beaten=EXCLUDED.total_distance_beaten,
                industry_sp=EXCLUDED.industry_sp,
                betfair_win_sp=EXCLUDED.betfair_win_sp,
                betfair_place_sp=EXCLUDED.betfair_place_sp,
                official_rating=EXCLUDED.official_rating,
                ts=EXCLUDED.ts,
                rpr=EXCLUDED.rpr,
                tfr=EXCLUDED.tfr,
                tfig=EXCLUDED.tfig,
                in_play_high=EXCLUDED.in_play_high,
                in_play_low=EXCLUDED.in_play_low,
                in_race_comment=EXCLUDED.in_race_comment,
                tf_comment=EXCLUDED.tf_comment,
                tfr_view=EXCLUDED.tfr_view,
                race_id=EXCLUDED.race_id,
                horse_id=EXCLUDED.horse_id,
                jockey_id=EXCLUDED.jockey_id,
                trainer_id=EXCLUDED.trainer_id,
                owner_id=EXCLUDED.owner_id,
                sire_id=EXCLUDED.sire_id,
                dam_id=EXCLUDED.dam_id,
                race_time=EXCLUDED.race_time,
                race_date=EXCLUDED.race_date,
                race_title=EXCLUDED.race_title,
                race_type=EXCLUDED.race_type,
                race_class=EXCLUDED.race_class,
                distance=EXCLUDED.distance,
                distance_yards=EXCLUDED.distance_yards,
                distance_meters=EXCLUDED.distance_meters,
                distance_kilometers=EXCLUDED.distance_kilometers,
                conditions=EXCLUDED.conditions,
                going=EXCLUDED.going,
                number_of_runners=EXCLUDED.number_of_runners,
                hcap_range=EXCLUDED.hcap_range,
                age_range=EXCLUDED.age_range,
                surface=EXCLUDED.surface,
                total_prize_money=EXCLUDED.total_prize_money,
                first_place_prize_money=EXCLUDED.first_place_prize_money,
                winning_time=EXCLUDED.winning_time,
                time_seconds=EXCLUDED.time_seconds,
                relative_time=EXCLUDED.relative_time,
                relative_to_standard=EXCLUDED.relative_to_standard,
                country=EXCLUDED.country,
                main_race_comment=EXCLUDED.main_race_comment,
                meeting_id=EXCLUDED.meeting_id,
                course_id=EXCLUDED.course_id,
                course=EXCLUDED.course,
                dam=EXCLUDED.dam,
                sire=EXCLUDED.sire,
                trainer=EXCLUDED.trainer,
                jockey=EXCLUDED.jockey,
                created_at=EXCLUDED.created_at;
            """

    @staticmethod
    def get_results_data_upsert_sql():
        return ResultsDataSQLGenerator.define_upsert_sql("results_data")
