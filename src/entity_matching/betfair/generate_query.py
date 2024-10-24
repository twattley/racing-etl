class MatchingBetfairSQLGenerator:
    @staticmethod
    def define_upsert_sql():
        return """
            INSERT INTO entities.todays_betfair_horse_ids (horse_id, bf_horse_id)
            SELECT horse_id, bf_horse_id
            FROM
                entities_todays_betfair_horse_ids_tmp_load
            ON CONFLICT ON CONSTRAINT todays_betfair_ids_horse_id_pd
            DO NOTHING;
            """
