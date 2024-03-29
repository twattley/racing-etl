--
-- PostgreSQL database dump
--

-- Dumped from database version 16.2 (Postgres.app)
-- Dumped by pg_dump version 16.2 (Postgres.app)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: metrics; Type: SCHEMA; Schema: -; Owner: doadmin
--

CREATE SCHEMA metrics;


ALTER SCHEMA metrics OWNER TO doadmin;

--
-- Name: rp_raw; Type: SCHEMA; Schema: -; Owner: doadmin
--

CREATE SCHEMA rp_raw;


ALTER SCHEMA rp_raw OWNER TO doadmin;

--
-- Name: staging; Type: SCHEMA; Schema: -; Owner: doadmin
--

CREATE SCHEMA staging;


ALTER SCHEMA staging OWNER TO doadmin;

--
-- Name: tf_raw; Type: SCHEMA; Schema: -; Owner: doadmin
--

CREATE SCHEMA tf_raw;


ALTER SCHEMA tf_raw OWNER TO doadmin;

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: refresh_missing_entity_counts(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
--

CREATE PROCEDURE metrics.refresh_missing_entity_counts()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.missing_entity_counts;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.missing_entity_counts
    SELECT * FROM metrics.missing_entity_counts_vw;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_missing_entity_counts() OWNER TO doadmin;

--
-- Name: refresh_missing_record_counts(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
--

CREATE PROCEDURE metrics.refresh_missing_record_counts()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.missing_record_counts;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.missing_record_counts
    SELECT * FROM metrics.missing_record_counts_vw;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_missing_record_counts() OWNER TO doadmin;

--
-- Name: insert_transformed_performance_data(); Type: PROCEDURE; Schema: public; Owner: doadmin
--

CREATE PROCEDURE public.insert_transformed_performance_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.performance_data
    (
        race_time, race_date, horse_name, age, draw, headgear,
        weight_carried_st_lbs, weight_carried_lbs, extra_weight_lbs,
        jockey_claim, finishing_position, industry_sp, betfair_win_sp,
        betfair_place_sp, official_rating, ts, rpr, tfr, tfig,
        in_play_high, in_play_low, in_race_comment, tf_comment,
        tf_rating_view, course_id, horse_id, jockey_id, trainer_id,
        owner_id, sire_id, dam_id, unique_id, created_at
    )
    SELECT
        race_time, race_date, horse_name, age, draw, headgear,
        weight_carried_st_lbs, weight_carried_lbs, extra_weight_lbs,
        jockey_claim, finishing_position, industry_sp, betfair_win_sp,
        betfair_place_sp, official_rating, ts, rpr, tfr, tfig,
        in_play_high, in_play_low, in_race_comment, tf_comment,
        tf_rating_view, course_id, horse_id, jockey_id, trainer_id,
        owner_id, sire_id, dam_id, unique_id, created_at
    FROM staging.transformed_performance_data
    ON CONFLICT (unique_id) DO UPDATE SET
        race_time = EXCLUDED.race_time,
        race_date = EXCLUDED.race_date,
        horse_name = EXCLUDED.horse_name,
        age = EXCLUDED.age,
        draw = EXCLUDED.draw,
        headgear = EXCLUDED.headgear,
        weight_carried_st_lbs = EXCLUDED.weight_carried_st_lbs,
        weight_carried_lbs = EXCLUDED.weight_carried_lbs,
        extra_weight_lbs = EXCLUDED.extra_weight_lbs,
        jockey_claim = EXCLUDED.jockey_claim,
        finishing_position = EXCLUDED.finishing_position,
        industry_sp = EXCLUDED.industry_sp,
        betfair_win_sp = EXCLUDED.betfair_win_sp,
        betfair_place_sp = EXCLUDED.betfair_place_sp,
        official_rating = EXCLUDED.official_rating,
        ts = EXCLUDED.ts,
        rpr = EXCLUDED.rpr,
        tfr = EXCLUDED.tfr,
        tfig = EXCLUDED.tfig,
        in_play_high = EXCLUDED.in_play_high,
        in_play_low = EXCLUDED.in_play_low,
        in_race_comment = EXCLUDED.in_race_comment,
        tf_comment = EXCLUDED.tf_comment,
        tf_rating_view = EXCLUDED.tf_rating_view,
        course_id = EXCLUDED.course_id,
        horse_id = EXCLUDED.horse_id,
        jockey_id = EXCLUDED.jockey_id,
        trainer_id = EXCLUDED.trainer_id,
        owner_id = EXCLUDED.owner_id,
        sire_id = EXCLUDED.sire_id,
        dam_id = EXCLUDED.dam_id,
        created_at = NOW(); 
END;
$$;


ALTER PROCEDURE public.insert_transformed_performance_data() OWNER TO doadmin;

--
-- Name: insert_transformed_race_data(); Type: PROCEDURE; Schema: public; Owner: doadmin
--

CREATE PROCEDURE public.insert_transformed_race_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.race
    (
        race_time, race_date, race_title, race_type, distance, distance_yards,
        distance_meters, distance_kilometers, conditions, going, number_of_runners, hcap_range,
        age_range, surface, prize, total_prize_money, first_place_prize_money,
        winning_time, time_seconds, relative_time, relative_to_standard, course_name,
        country, main_race_comment, meeting_id, race_id, course_id, created_at
    )
    SELECT
        race_time, race_date, race_title, race_type, distance, distance_yards,
        distance_meters, distance_kilometers, conditions, going, number_of_runners, hcap_range,
        age_range, surface, prize, total_prize_money, first_place_prize_money,
        winning_time, time_seconds, relative_time, relative_to_standard, course_name,
        country, main_race_comment, meeting_id, race_id, course_id, created_at
    FROM staging.transformed_race_data
    ON CONFLICT (race_id) DO UPDATE SET
        race_time = EXCLUDED.race_time,
        race_date = EXCLUDED.race_date,
        race_title = EXCLUDED.race_title,
        race_type = EXCLUDED.race_type,
        distance = EXCLUDED.distance,
        distance_yards = EXCLUDED.distance_yards,
        distance_meters = EXCLUDED.distance_meters,
        distance_kilometers = EXCLUDED.distance_kilometers,
        conditions = EXCLUDED.conditions,
        going = EXCLUDED.going,
        number_of_runners = EXCLUDED.number_of_runners,
        hcap_range = EXCLUDED.hcap_range,
        age_range = EXCLUDED.age_range,
        surface = EXCLUDED.surface,
        prize = EXCLUDED.prize,
        total_prize_money = EXCLUDED.total_prize_money,
        first_place_prize_money = EXCLUDED.first_place_prize_money,
        winning_time = EXCLUDED.winning_time,
        time_seconds = EXCLUDED.time_seconds,
        relative_time = EXCLUDED.relative_time,
        relative_to_standard = EXCLUDED.relative_to_standard,
        course_name = EXCLUDED.course_name,
        country = EXCLUDED.country,
        main_race_comment = EXCLUDED.main_race_comment,
        race_id = EXCLUDED.race_id,
        course_id = EXCLUDED.course_id,
        created_at = NOW(); 
END;
$$;


ALTER PROCEDURE public.insert_transformed_race_data() OWNER TO doadmin;

--
-- Name: load_direct_matches(); Type: PROCEDURE; Schema: public; Owner: doadmin
--

CREATE PROCEDURE public.load_direct_matches()
    LANGUAGE plpgsql
    AS $$
BEGIN
	CALL staging.insert_owner_data();
	CALL staging.insert_formatted_sire_data();
	CALL staging.insert_formatted_dam_data();
	CALL staging.insert_formatted_horse_data();
	
END;
$$;


ALTER PROCEDURE public.load_direct_matches() OWNER TO doadmin;

--
-- Name: load_fuzzy_matches(); Type: PROCEDURE; Schema: public; Owner: doadmin
--

CREATE PROCEDURE public.load_fuzzy_matches()
    LANGUAGE plpgsql
    AS $$
BEGIN
	CALL staging.insert_matched_jockey_data();
	CALL staging.insert_matched_trainer_data();
	CALL staging.insert_matched_sire_data();
	CALL staging.insert_matched_dam_data();
	CALL staging.insert_matched_horse_data();
END;
$$;


ALTER PROCEDURE public.load_fuzzy_matches() OWNER TO doadmin;

--
-- Name: get_my_list(); Type: FUNCTION; Schema: rp_raw; Owner: doadmin
--

CREATE FUNCTION rp_raw.get_my_list() RETURNS text[]
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN ARRAY['value1', 'value2', 'value3'];
END;
$$;


ALTER FUNCTION rp_raw.get_my_list() OWNER TO doadmin;

--
-- Name: insert_formatted_dam_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_formatted_dam_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	TRUNCATE staging.dam;
	
    WITH formatted_tf AS (
        SELECT
            dam_name,
            dam_id,
            race_date,
            regexp_replace(lower(regexp_replace(dam_name, '\s*\([^)]*\)', '', 'g')), '\s+', '', 'g') AS formatted_dam_name
        FROM
            tf_raw.performance_data
    ),
    formatted_rp AS (
        SELECT
            dam_name,
            dam_id,
            race_date,
            regexp_replace(lower(regexp_replace(dam_name, '\s*\([^)]*\)', '', 'g')), '\s+', '', 'g') AS formatted_dam_name
        FROM
            rp_raw.performance_data
    ),
    dam_dates AS (
        SELECT
            DISTINCT tf.formatted_dam_name,
            tf.dam_id AS tf_dam_id,
            tf.race_date,
            rp.dam_name AS rp_dam_name,
            rp.dam_id AS rp_dam_id
        FROM
            formatted_tf tf
        LEFT JOIN
            formatted_rp rp ON tf.formatted_dam_name = rp.formatted_dam_name AND tf.race_date = rp.race_date
    ),
    matches AS (
        SELECT
            DISTINCT rp_dam_id,
            rp_dam_name,
            tf_dam_id
        FROM
            dam_dates
        WHERE
            rp_dam_id IS NOT NULL AND rp_dam_name IS NOT NULL AND tf_dam_id IS NOT NULL
    )
    
    INSERT INTO staging.dam (rp_id, name, tf_id)
    SELECT rp_dam_id, rp_dam_name, tf_dam_id
    FROM matches;
	
	INSERT INTO public.dam (rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.dam
	ON CONFLICT (rp_id, tf_id) DO NOTHING;
	
END;
$$;


ALTER PROCEDURE staging.insert_formatted_dam_data() OWNER TO doadmin;

--
-- Name: insert_formatted_horse_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_formatted_horse_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

    TRUNCATE staging.horse;
    
    WITH formatted_tf AS (
        SELECT
            horse_name,
            horse_id,
            race_date,
            regexp_replace(lower(regexp_replace(horse_name, '\s*\([^)]*\)', '', 'g')), '\s+', '', 'g') AS formatted_horse_name
        FROM
            tf_raw.performance_data
    ),
    formatted_rp AS (
        SELECT
            horse_name,
            horse_id,
            race_date,
            regexp_replace(lower(regexp_replace(horse_name, '\s*\([^)]*\)', '', 'g')), '\s+', '', 'g') AS formatted_horse_name
        FROM
            rp_raw.performance_data
    ),
    horse_dates AS (
        SELECT
            DISTINCT tf.formatted_horse_name,
            tf.horse_id AS tf_horse_id,
            tf.race_date,
            rp.horse_name AS rp_horse_name,
            rp.horse_id AS rp_horse_id
        FROM
            formatted_tf tf
        LEFT JOIN
            formatted_rp rp ON tf.formatted_horse_name = rp.formatted_horse_name AND tf.race_date = rp.race_date
    ),
    matches AS (
        SELECT
            DISTINCT rp_horse_id,
            rp_horse_name,
            tf_horse_id
        FROM
            horse_dates
        WHERE
            rp_horse_id IS NOT NULL AND rp_horse_name IS NOT NULL AND tf_horse_id IS NOT NULL
    )
    
    INSERT INTO staging.horse (rp_id, name, tf_id)
    SELECT rp_horse_id, rp_horse_name, tf_horse_id
    FROM matches;
    
    INSERT INTO public.horse(rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.horse
    ON CONFLICT (rp_id, tf_id) DO NOTHING;

END;
$$;


ALTER PROCEDURE staging.insert_formatted_horse_data() OWNER TO doadmin;

--
-- Name: insert_formatted_sire_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_formatted_sire_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	TRUNCATE staging.sire;
	
    WITH formatted_tf AS (
        SELECT
            sire_name,
            sire_id,
            race_date,
            regexp_replace(lower(regexp_replace(sire_name, '\s*\([^)]*\)', '', 'g')), '\s+', '', 'g') AS formatted_sire_name
        FROM
            tf_raw.performance_data
    ),
    formatted_rp AS (
        SELECT
            sire_name,
            sire_id,
            race_date,
            regexp_replace(lower(regexp_replace(sire_name, '\s*\([^)]*\)', '', 'g')), '\s+', '', 'g') AS formatted_sire_name
        FROM
            rp_raw.performance_data
    ),
    sire_dates AS (
        SELECT
            DISTINCT tf.formatted_sire_name,
            tf.sire_id AS tf_sire_id,
            tf.race_date,
            rp.sire_name AS rp_sire_name,
            rp.sire_id AS rp_sire_id
        FROM
            formatted_tf tf
        LEFT JOIN
            formatted_rp rp ON tf.formatted_sire_name = rp.formatted_sire_name AND tf.race_date = rp.race_date
    ),
    matches AS (
        SELECT
            DISTINCT rp_sire_id,
            rp_sire_name,
            tf_sire_id
        FROM
            sire_dates
        WHERE
            rp_sire_id IS NOT NULL AND rp_sire_name IS NOT NULL AND tf_sire_id IS NOT NULL
    )
    
    INSERT INTO staging.sire (rp_id, name, tf_id)
    SELECT rp_sire_id, rp_sire_name, tf_sire_id
    FROM matches;
	
	INSERT INTO public.sire (rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.sire
	ON CONFLICT (rp_id, tf_id) DO NOTHING;
	
END;
$$;


ALTER PROCEDURE staging.insert_formatted_sire_data() OWNER TO doadmin;

--
-- Name: insert_into_joined_performance_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_into_joined_performance_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO staging.joined_performance_data (
		horse_name,
		horse_age,
		jockey_name,
		jockey_claim,
		trainer_name,
		owner_name,
		horse_weight,
		draw,
		finishing_position,
		age,
		official_rating,
		ts,
		rpr,
		industry_sp,
		extra_weight,
		headgear,
		in_race_comment,
		sire_name,
		dam_name,
		dams_sire,
		race_date,
		race_title,
		race_time,
		race_timestamp,
		conditions,
		distance,
		going,
		winning_time,
		number_of_runners,
		total_prize_money,
		first_place_prize_money,
		currency,
		country,
		surface,
		tfr,
		tfig,
		betfair_win_sp,
		betfair_place_sp,
		in_play_prices,
		tf_comment,
		hcap_range,
		age_range,
		race_type,
		main_race_comment,
		meeting_id,
		course_id,
		horse_id,
		sire_id,
		dam_id,
		trainer_id,
		jockey_id,
		race_id,
		unique_id,
		created_at
	)
WITH unique_course AS (
    SELECT DISTINCT ON (rp_course_id) *
    FROM course
),
unique_horse AS (
    SELECT DISTINCT ON (rp_id) *
    FROM horse
),
unique_sire AS (
    SELECT DISTINCT ON (rp_id) *
    FROM sire
),
unique_dam AS (
    SELECT DISTINCT ON (rp_id) *
    FROM dam
),
unique_trainer AS (
    SELECT DISTINCT ON (rp_id) *
    FROM trainer
),
unique_jockey AS (
    SELECT DISTINCT ON (rp_id) *
    FROM jockey
),
new_rp_records AS (
    SELECT *
	FROM staging.missing_performance_data_vw
),
joined_data AS (
    SELECT 
		nrr.horse_name as horse_name,
		nrr.horse_age as horse_age,
		nrr.jockey_name as jockey_name,
		nrr.jockey_claim as jockey_claim,
		nrr.trainer_name as trainer_name,
		nrr.owner_name as owner_name,
		nrr.horse_weight,
		nrr.draw,
		COALESCE(nrr.finishing_position, tf.finishing_position) AS finishing_position,
		COALESCE(nrr.horse_age, tf.horse_age) AS age,
		COALESCE(nrr.or_value, tf.official_rating) AS official_rating,
		nrr.ts_value as ts,
		nrr.rpr_value as rpr,
		nrr.horse_price as industry_sp,
		nrr.extra_weight as extra_weight,
		nrr.headgear,
		nrr.comment as in_race_comment,
		nrr.sire_name as sire_name,
		nrr.dam_name as dam_name,
		nrr.dams_sire as dams_sire,
		nrr.race_date as race_date,
		nrr.race_title as race_title,
		nrr.race_time as race_time,
		nrr.race_timestamp as race_timestamp,
		nrr.conditions as conditions,
		nrr.distance as distance,
		nrr.going as going,
		nrr.winning_time as winning_time,
		nrr.number_of_runners as number_of_runners,
		nrr.total_prize_money as total_prize_money,
		nrr.first_place_prize_money,
		nrr.currency,
		nrr.course_name as course_name,
		nrr.country as country,
		nrr.surface as surface,
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
		nrr.meeting_id as meeting_id,
		c.course_id as course_id,
		h.id as horse_id,
		s.id as sire_id,
		d.id as dam_id,
		t.id as trainer_id,
		j.id as jockey_id,
		nrr.race_id as race_id,
		nrr.unique_id as unique_id,
        ROW_NUMBER() OVER(PARTITION BY nrr.unique_id ORDER BY nrr.unique_id) AS rn
    FROM new_rp_records nrr
	LEFT JOIN course c ON nrr.course_id = c.rp_course_id
	LEFT JOIN horse h ON nrr.horse_id::integer = h.rp_id
	LEFT JOIN sire s ON nrr.sire_id::integer = s.rp_id
	LEFT JOIN dam d ON nrr.dam_id::integer = d.rp_id
	LEFT JOIN trainer t ON nrr.trainer_id::integer = t.rp_id
	LEFT JOIN jockey j ON nrr.jockey_id::integer = j.rp_id
    LEFT JOIN tf_raw.performance_data tf ON tf.horse_id = h.tf_id
		AND tf.jockey_id = j.tf_id
		AND tf.trainer_id = t.tf_id
		AND tf.dam_id = d.tf_id
		AND tf.sire_id = s.tf_id
		AND tf.race_date = nrr.race_date
		AND tf.course_id = c.tf_course_id
    WHERE tf.unique_id IS NOT NULL
)
SELECT 	horse_name,
		horse_age,
		jockey_name,
		jockey_claim,
		trainer_name,
		owner_name,
		horse_weight,
		draw,
		finishing_position,
		age,
		official_rating,
		ts,
		rpr,
		industry_sp,
		extra_weight,
		headgear,
		in_race_comment,
		sire_name,
		dam_name,
		dams_sire,
		race_date,
		race_title,
		race_time,
		race_timestamp,
		conditions,
		distance,
		going,
		winning_time,
		number_of_runners,
		total_prize_money,
		first_place_prize_money,
		currency,
		country,
		surface,
		tfr,
		tfig,
		betfair_win_sp,
		betfair_place_sp,
		in_play_prices,
		tf_comment,
		hcap_range,
		age_range,
		race_type,
		main_race_comment,
		meeting_id,
		course_id,
		horse_id,
		sire_id,
		dam_id,
		trainer_id,
		jockey_id,
		race_id,
		unique_id,
		now()
	FROM joined_data WHERE rn = 1;
END;
$$;


ALTER PROCEDURE staging.insert_into_joined_performance_data() OWNER TO doadmin;

--
-- Name: insert_matched_dam_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_matched_dam_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	INSERT INTO public.dam (rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.dam
	ON CONFLICT (rp_id, tf_id) DO NOTHING;
	
END;
$$;


ALTER PROCEDURE staging.insert_matched_dam_data() OWNER TO doadmin;

--
-- Name: insert_matched_horse_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_matched_horse_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	INSERT INTO public.horse (rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.horse
	ON CONFLICT (rp_id, tf_id) DO NOTHING;
	
END;
$$;


ALTER PROCEDURE staging.insert_matched_horse_data() OWNER TO doadmin;

--
-- Name: insert_matched_jockey_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_matched_jockey_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	INSERT INTO public.jockey (rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.jockey
	ON CONFLICT (rp_id, tf_id) DO NOTHING;
	
END;
$$;


ALTER PROCEDURE staging.insert_matched_jockey_data() OWNER TO doadmin;

--
-- Name: insert_matched_sire_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_matched_sire_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	INSERT INTO public.sire (rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.sire
	ON CONFLICT (rp_id, tf_id) DO NOTHING;
	
END;
$$;


ALTER PROCEDURE staging.insert_matched_sire_data() OWNER TO doadmin;

--
-- Name: insert_matched_trainer_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_matched_trainer_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	INSERT INTO public.trainer (rp_id, name, tf_id)
    SELECT rp_id::integer, name, tf_id
    FROM staging.trainer
	ON CONFLICT (rp_id, tf_id) DO NOTHING;
	
END;
$$;


ALTER PROCEDURE staging.insert_matched_trainer_data() OWNER TO doadmin;

--
-- Name: insert_owner_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
--

CREATE PROCEDURE staging.insert_owner_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
WITH owners AS (
    SELECT DISTINCT
        rp.owner_id::integer,  -- Cast owner_id to integer
        rp.owner_name
    FROM
        rp_raw.performance_data rp
    LEFT JOIN public.owner o ON
        rp.owner_id::integer = o.rp_id  -- Cast owner_id to integer here as well
    WHERE
        o.name IS NULL
)
INSERT INTO public.owner (rp_id, name)
SELECT
    owner_id,
    owner_name
FROM
    owners
ON CONFLICT (rp_id, name) DO NOTHING;
END;
$$;


ALTER PROCEDURE staging.insert_owner_data() OWNER TO doadmin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: course; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.course (
    name character varying(132),
    id integer NOT NULL,
    rp_name character varying(132),
    rp_id character varying(4),
    tf_name character varying(132),
    tf_id character varying(4),
    country_id character varying(4)
);


ALTER TABLE public.course OWNER TO doadmin;

--
-- Name: dam; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.dam (
    rp_id integer,
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.dam OWNER TO doadmin;

--
-- Name: horse; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.horse (
    rp_id integer,
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.horse OWNER TO doadmin;

--
-- Name: jockey; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.jockey (
    rp_id integer,
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.jockey OWNER TO doadmin;

--
-- Name: sire; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.sire (
    rp_id integer,
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.sire OWNER TO doadmin;

--
-- Name: trainer; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.trainer (
    rp_id integer,
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.trainer OWNER TO doadmin;

--
-- Name: performance_data; Type: TABLE; Schema: rp_raw; Owner: doadmin
--

CREATE TABLE rp_raw.performance_data (
    horse_id character varying(32),
    horse_name character varying(132),
    horse_age character varying(16),
    jockey_id character varying(32),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_id character varying(32),
    trainer_name character varying(132),
    owner_id character varying(32),
    owner_name character varying(132),
    horse_weight character varying(16),
    or_value character varying(16),
    finishing_position character varying(16),
    draw character varying(16),
    ts_value character varying(16),
    rpr_value character varying(16),
    horse_price character varying(16),
    extra_weight numeric,
    headgear character varying(16),
    comment text,
    sire_name character varying(132),
    sire_id character varying(32),
    dam_name character varying(132),
    dam_id character varying(32),
    dams_sire character varying(132),
    dams_sire_id character varying(32),
    race_date character varying(32),
    race_title text,
    race_time character varying(32),
    race_timestamp timestamp without time zone,
    conditions character varying(32),
    distance character varying(32),
    going character varying(32),
    winning_time character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    currency character varying(16),
    course_id character varying(32),
    course_name character varying(132),
    course character varying(132),
    debug_link text,
    race_id character varying(32),
    country character varying(16),
    surface character varying(16),
    created_at timestamp without time zone,
    unique_id character varying(132),
    meeting_id character varying(132)
);


ALTER TABLE rp_raw.performance_data OWNER TO doadmin;

--
-- Name: base_formatted_entities; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.base_formatted_entities AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*I\s*$|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN ((pd.finishing_position)::text ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace((pd.sire_name)::text, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace((pd.dam_name)::text, '\s*I\s*$|\s*\d+[A-Za-z]*'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.id AS course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (rp_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_id)::text)))
  WHERE (pd.race_timestamp > (now() - '5 years'::interval));


ALTER VIEW rp_raw.base_formatted_entities OWNER TO doadmin;

--
-- Name: unmatched_dams; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.unmatched_dams AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    d.rp_id,
    d.name,
    d.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.dam d ON (((be.dam_id)::text = (d.rp_id)::text)))
  WHERE (d.rp_id IS NULL);


ALTER VIEW rp_raw.unmatched_dams OWNER TO doadmin;

--
-- Name: unmatched_horses; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.unmatched_horses AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    h.rp_id,
    h.name,
    h.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.horse h ON (((be.horse_id)::text = (h.rp_id)::text)))
  WHERE (h.rp_id IS NULL);


ALTER VIEW rp_raw.unmatched_horses OWNER TO doadmin;

--
-- Name: unmatched_jockeys; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.unmatched_jockeys AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    j.rp_id,
    j.name,
    j.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.jockey j ON (((be.jockey_id)::text = (j.rp_id)::text)))
  WHERE (j.rp_id IS NULL);


ALTER VIEW rp_raw.unmatched_jockeys OWNER TO doadmin;

--
-- Name: unmatched_sires; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.unmatched_sires AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    s.rp_id,
    s.name,
    s.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.sire s ON (((be.sire_id)::text = (s.rp_id)::text)))
  WHERE (s.rp_id IS NULL);


ALTER VIEW rp_raw.unmatched_sires OWNER TO doadmin;

--
-- Name: unmatched_trainers; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.unmatched_trainers AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    t.rp_id,
    t.name,
    t.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.trainer t ON (((be.trainer_id)::text = (t.rp_id)::text)))
  WHERE (t.rp_id IS NULL);


ALTER VIEW rp_raw.unmatched_trainers OWNER TO doadmin;

--
-- Name: performance_data; Type: TABLE; Schema: tf_raw; Owner: doadmin
--

CREATE TABLE tf_raw.performance_data (
    tf_rating character varying(16),
    tf_speed_figure character varying(16),
    draw character varying(16),
    trainer_name character varying(132),
    trainer_id character varying(32),
    jockey_name character varying(132),
    jockey_id character varying(32),
    sire_name character varying(132),
    sire_id character varying(32),
    dam_name character varying(132),
    dam_id character varying(32),
    finishing_position character varying(16),
    horse_name character varying(132),
    horse_id character varying(32),
    horse_name_link character varying(132),
    horse_age character varying(16),
    equipment character varying(16),
    official_rating character varying(16),
    fractional_price character varying(64),
    betfair_win_sp character varying(16),
    betfair_place_sp character varying(16),
    in_play_prices character varying(16),
    tf_comment text,
    course text,
    race_date character varying(32),
    race_time character varying(32),
    race_timestamp timestamp without time zone,
    course_id character varying(32),
    race text,
    race_id character varying(132),
    distance character varying(32),
    going character varying(16),
    prize character varying(16),
    hcap_range character varying(16),
    age_range character varying(16),
    race_type character varying(16),
    main_race_comment text,
    debug_link text,
    created_at timestamp without time zone,
    sire_name_link character varying(132),
    unique_id character varying(132)
);


ALTER TABLE tf_raw.performance_data OWNER TO doadmin;

--
-- Name: base_formatted_entities; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.base_formatted_entities AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*I\s*$|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN ((pd.finishing_position)::text ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace((pd.sire_name)::text, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace((pd.dam_name)::text, '\s*I\s*$|\s*\d+[A-Za-z]*'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.id AS course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_id)::text)))
  WHERE (pd.race_timestamp > (now() - '5 years'::interval));


ALTER VIEW tf_raw.base_formatted_entities OWNER TO doadmin;

--
-- Name: unmatched_dams; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_dams AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    d.rp_id,
    d.name,
    d.tf_id
   FROM (tf_raw.base_formatted_entities be
     LEFT JOIN public.dam d ON (((be.dam_id)::text = (d.tf_id)::text)))
  WHERE (d.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_dams OWNER TO doadmin;

--
-- Name: unmatched_horses; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_horses AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    h.rp_id,
    h.name,
    h.tf_id
   FROM (tf_raw.base_formatted_entities be
     LEFT JOIN public.horse h ON (((be.horse_id)::text = (h.tf_id)::text)))
  WHERE (h.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_horses OWNER TO doadmin;

--
-- Name: unmatched_jockeys; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_jockeys AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    j.rp_id,
    j.name,
    j.tf_id
   FROM (tf_raw.base_formatted_entities be
     LEFT JOIN public.jockey j ON (((be.jockey_id)::text = (j.tf_id)::text)))
  WHERE (j.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_jockeys OWNER TO doadmin;

--
-- Name: unmatched_sires; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_sires AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    s.rp_id,
    s.name,
    s.tf_id
   FROM (tf_raw.base_formatted_entities be
     LEFT JOIN public.sire s ON (((be.sire_id)::text = (s.tf_id)::text)))
  WHERE (s.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_sires OWNER TO doadmin;

--
-- Name: unmatched_trainers; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_trainers AS
 SELECT be.horse_name,
    be.filtered_horse_name,
    be.horse_id,
    be.horse_age,
    be.jockey_id,
    be.jockey_name,
    be.filtered_jockey_name,
    be.trainer_id,
    be.trainer_name,
    be.filtered_trainer_name,
    be.converted_finishing_position,
    be.sire_name,
    be.filtered_sire_name,
    be.sire_id,
    be.dam_name,
    be.filtered_dam_name,
    be.dam_id,
    be.race_timestamp,
    be.course_id,
    be.unique_id,
    be.race_date,
    t.rp_id,
    t.name,
    t.tf_id
   FROM (tf_raw.base_formatted_entities be
     LEFT JOIN public.trainer t ON (((be.trainer_id)::text = (t.tf_id)::text)))
  WHERE (t.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_trainers OWNER TO doadmin;

--
-- Name: missing_entity_counts_vw; Type: VIEW; Schema: metrics; Owner: doadmin
--

CREATE VIEW metrics.missing_entity_counts_vw AS
 SELECT 'missing_tf_dams'::text AS missing_entity,
    count(DISTINCT unmatched_dams.dam_name) AS count
   FROM tf_raw.unmatched_dams
UNION
 SELECT 'missing_tf_sires'::text AS missing_entity,
    count(DISTINCT unmatched_sires.sire_name) AS count
   FROM tf_raw.unmatched_sires
UNION
 SELECT 'missing_tf_trainers'::text AS missing_entity,
    count(DISTINCT unmatched_trainers.trainer_name) AS count
   FROM tf_raw.unmatched_trainers
UNION
 SELECT 'missing_tf_jockeys'::text AS missing_entity,
    count(DISTINCT unmatched_jockeys.jockey_name) AS count
   FROM tf_raw.unmatched_jockeys
UNION
 SELECT 'missing_tf_horses'::text AS missing_entity,
    count(DISTINCT unmatched_horses.horse_name) AS count
   FROM tf_raw.unmatched_horses
UNION
 SELECT 'missing_rp_dams'::text AS missing_entity,
    count(DISTINCT unmatched_dams.dam_name) AS count
   FROM rp_raw.unmatched_dams
UNION
 SELECT 'missing_rp_sires'::text AS missing_entity,
    count(DISTINCT unmatched_sires.sire_name) AS count
   FROM rp_raw.unmatched_sires
UNION
 SELECT 'missing_rp_trainers'::text AS missing_entity,
    count(DISTINCT unmatched_trainers.trainer_name) AS count
   FROM rp_raw.unmatched_trainers
UNION
 SELECT 'missing_rp_jockeys'::text AS missing_entity,
    count(DISTINCT unmatched_jockeys.jockey_name) AS count
   FROM rp_raw.unmatched_jockeys
UNION
 SELECT 'missing_rp_horses'::text AS missing_entity,
    count(DISTINCT unmatched_horses.horse_name) AS count
   FROM rp_raw.unmatched_horses;


ALTER VIEW metrics.missing_entity_counts_vw OWNER TO doadmin;

--
-- Name: record_count_differences_vw; Type: VIEW; Schema: metrics; Owner: doadmin
--

CREATE VIEW metrics.record_count_differences_vw AS
 WITH rp_course_counts AS (
         SELECT c.name AS course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.id AS course_id
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_id)::text)))
          GROUP BY c.name, pd.race_date, c.id
        ), tf_course_counts AS (
         SELECT c.name AS course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.id AS course_id
           FROM (tf_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_id)::text)))
          GROUP BY c.name, pd.race_date, c.id
        )
 SELECT COALESCE(rp.course_name, tf.course_name) AS course,
    COALESCE(rp.race_date, tf.race_date) AS race_date,
    rp.num_records AS rp_num_records,
    tf.num_records AS tf_num_records
   FROM (rp_course_counts rp
     JOIN tf_course_counts tf ON ((((rp.race_date)::text = (tf.race_date)::text) AND ((rp.course_id)::text = (tf.course_id)::text))))
  WHERE ((rp.num_records <> tf.num_records) OR (rp.num_records IS NULL) OR (tf.num_records IS NULL))
  ORDER BY COALESCE(rp.race_date, tf.race_date) DESC, COALESCE(rp.course_name, tf.course_name), rp.course_id;


ALTER VIEW metrics.record_count_differences_vw OWNER TO doadmin;

--
-- Name: record_count_differences_gt_2010_vw; Type: VIEW; Schema: metrics; Owner: doadmin
--

CREATE VIEW metrics.record_count_differences_gt_2010_vw AS
 SELECT course,
    race_date,
    rp_num_records,
    tf_num_records
   FROM metrics.record_count_differences_vw
  WHERE ((race_date)::date > '2010-01-01'::date);


ALTER VIEW metrics.record_count_differences_gt_2010_vw OWNER TO doadmin;

--
-- Name: country; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.country (
    country_id integer,
    country_name character varying(132)
);


ALTER TABLE public.country OWNER TO doadmin;

--
-- Name: dam_dam_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.dam_dam_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dam_dam_id_seq OWNER TO doadmin;

--
-- Name: dam_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.dam_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dam_id_seq OWNER TO doadmin;

--
-- Name: dam_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: doadmin
--

ALTER SEQUENCE public.dam_id_seq OWNED BY public.dam.id;


--
-- Name: horse_horse_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.horse_horse_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.horse_horse_id_seq OWNER TO doadmin;

--
-- Name: horse_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.horse_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.horse_id_seq OWNER TO doadmin;

--
-- Name: horse_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: doadmin
--

ALTER SEQUENCE public.horse_id_seq OWNED BY public.horse.id;


--
-- Name: jockey_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.jockey_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.jockey_id_seq OWNER TO doadmin;

--
-- Name: jockey_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: doadmin
--

ALTER SEQUENCE public.jockey_id_seq OWNED BY public.jockey.id;


--
-- Name: jockey_jockey_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.jockey_jockey_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.jockey_jockey_id_seq OWNER TO doadmin;

--
-- Name: performance_data; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.performance_data (
    race_time timestamp without time zone NOT NULL,
    race_date date NOT NULL,
    horse_name character varying(132) NOT NULL,
    age integer,
    draw integer,
    headgear character varying(64),
    weight_carried_st_lbs character varying(16),
    weight_carried_lbs smallint,
    extra_weight_lbs smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    industry_sp character varying(16),
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    official_rating smallint,
    ts smallint,
    rpr smallint,
    tfr smallint,
    tfig smallint,
    in_play_high numeric(6,2),
    in_play_low numeric(6,2),
    in_race_comment text,
    tf_comment text,
    tf_rating_view character varying(16),
    course_id smallint NOT NULL,
    horse_id integer NOT NULL,
    jockey_id integer NOT NULL,
    trainer_id integer NOT NULL,
    owner_id integer NOT NULL,
    sire_id integer NOT NULL,
    dam_id integer NOT NULL,
    unique_id character varying(132) NOT NULL,
    created_at timestamp without time zone NOT NULL
);


ALTER TABLE public.performance_data OWNER TO doadmin;

--
-- Name: joined_performance_data; Type: TABLE; Schema: staging; Owner: postgres
--

CREATE TABLE staging.joined_performance_data (
    horse_name character varying(132),
    horse_age character varying(16),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    owner_name character varying(132),
    horse_weight character varying(16),
    draw character varying(16),
    finishing_position character varying(16),
    age character varying(16),
    official_rating character varying(16),
    ts character varying(16),
    rpr character varying(16),
    industry_sp character varying(16),
    extra_weight numeric,
    headgear character varying(16),
    in_race_comment text,
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    race_date character varying(32),
    race_title text,
    race_time character varying(32),
    race_timestamp timestamp without time zone,
    conditions character varying(32),
    distance character varying(32),
    going character varying(32),
    winning_time character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    currency character varying(16),
    country character varying(16),
    surface character varying(16),
    tfr character varying(16),
    tfig character varying(16),
    betfair_win_sp character varying(16),
    betfair_place_sp character varying(16),
    in_play_prices character varying(16),
    tf_comment text,
    hcap_range character varying(16),
    age_range character varying(16),
    race_type character varying(16),
    main_race_comment text,
    meeting_id character varying(132),
    course_id integer,
    horse_id integer,
    sire_id integer,
    dam_id integer,
    trainer_id integer,
    jockey_id integer,
    race_id character varying(32),
    unique_id character varying(132),
    created_at timestamp without time zone
);


ALTER TABLE staging.joined_performance_data OWNER TO postgres;

--
-- Name: missing_performance_data_vw; Type: VIEW; Schema: public; Owner: doadmin
--

CREATE VIEW public.missing_performance_data_vw AS
 SELECT spd.horse_name,
    spd.horse_age,
    spd.jockey_name,
    spd.jockey_claim,
    spd.trainer_name,
    spd.owner_name,
    spd.horse_weight,
    spd.draw,
    spd.finishing_position,
    spd.age,
    spd.official_rating,
    spd.ts,
    spd.rpr,
    spd.industry_sp,
    spd.extra_weight,
    spd.headgear,
    spd.in_race_comment,
    spd.sire_name,
    spd.dam_name,
    spd.dams_sire,
    spd.race_date,
    spd.race_title,
    spd.race_time,
    spd.race_timestamp,
    spd.conditions,
    spd.distance,
    spd.going,
    spd.winning_time,
    spd.number_of_runners,
    spd.total_prize_money,
    spd.first_place_prize_money,
    spd.currency,
    spd.country,
    spd.surface,
    spd.tfr,
    spd.tfig,
    spd.betfair_win_sp,
    spd.betfair_place_sp,
    spd.in_play_prices,
    spd.tf_comment,
    spd.hcap_range,
    spd.age_range,
    spd.race_type,
    spd.main_race_comment,
    spd.meeting_id,
    spd.course_id,
    spd.horse_id,
    spd.sire_id,
    spd.dam_id,
    spd.trainer_id,
    spd.jockey_id,
    spd.race_id,
    spd.unique_id,
    spd.created_at
   FROM (staging.joined_performance_data spd
     LEFT JOIN public.performance_data pd ON (((spd.unique_id)::text = (pd.unique_id)::text)))
  WHERE ((pd.unique_id IS NULL) AND ((spd.race_date)::text > '2010-01-01'::text));


ALTER VIEW public.missing_performance_data_vw OWNER TO doadmin;

--
-- Name: race; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.race (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    distance character varying(16),
    distance_yards numeric(10,2),
    distance_meters numeric(10,2),
    distance_kilometers numeric(10,2),
    conditions character varying(32),
    going character varying(32),
    number_of_runners smallint,
    hcap_range character varying(32),
    age_range character varying(32),
    surface character varying(32),
    prize character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    course_name character varying(132),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id character varying(132),
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE public.race OWNER TO doadmin;

--
-- Name: transformed_race_data; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.transformed_race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    distance character varying(16),
    distance_yards numeric(10,2),
    distance_meters numeric(10,2),
    distance_kilometers numeric(10,2),
    conditions character varying(32),
    going character varying(32),
    number_of_runners smallint,
    hcap_range character varying(32),
    age_range character varying(32),
    surface character varying(32),
    prize character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    course_name character varying(132),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id character varying(132),
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE staging.transformed_race_data OWNER TO doadmin;

--
-- Name: missing_race_data_vw; Type: VIEW; Schema: public; Owner: doadmin
--

CREATE VIEW public.missing_race_data_vw AS
 SELECT spd.race_time,
    spd.race_date,
    spd.race_title,
    spd.race_type,
    spd.distance,
    spd.distance_yards,
    spd.distance_meters,
    spd.distance_kilometers,
    spd.conditions,
    spd.going,
    spd.number_of_runners,
    spd.hcap_range,
    spd.age_range,
    spd.surface,
    spd.prize,
    spd.total_prize_money,
    spd.first_place_prize_money,
    spd.winning_time,
    spd.time_seconds,
    spd.relative_time,
    spd.relative_to_standard,
    spd.course_name,
    spd.country,
    spd.main_race_comment,
    spd.meeting_id,
    spd.race_id,
    spd.course_id,
    spd.created_at
   FROM (staging.transformed_race_data spd
     LEFT JOIN public.race pd ON (((spd.race_id)::text = (pd.race_id)::text)))
  WHERE (pd.race_id IS NULL);


ALTER VIEW public.missing_race_data_vw OWNER TO doadmin;

--
-- Name: owner; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.owner (
    rp_id integer,
    name text,
    id integer NOT NULL
);


ALTER TABLE public.owner OWNER TO doadmin;

--
-- Name: owner_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.owner_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.owner_id_seq OWNER TO doadmin;

--
-- Name: owner_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: doadmin
--

ALTER SEQUENCE public.owner_id_seq OWNED BY public.owner.id;


--
-- Name: sire_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.sire_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sire_id_seq OWNER TO doadmin;

--
-- Name: sire_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: doadmin
--

ALTER SEQUENCE public.sire_id_seq OWNED BY public.sire.id;


--
-- Name: sire_sire_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.sire_sire_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sire_sire_id_seq OWNER TO doadmin;

--
-- Name: trainer_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.trainer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainer_id_seq OWNER TO doadmin;

--
-- Name: trainer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: doadmin
--

ALTER SEQUENCE public.trainer_id_seq OWNED BY public.trainer.id;


--
-- Name: trainer_trainer_id_seq; Type: SEQUENCE; Schema: public; Owner: doadmin
--

CREATE SEQUENCE public.trainer_trainer_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainer_trainer_id_seq OWNER TO doadmin;

--
-- Name: days_results_links; Type: TABLE; Schema: rp_raw; Owner: doadmin
--

CREATE TABLE rp_raw.days_results_links (
    date date,
    link text
);


ALTER TABLE rp_raw.days_results_links OWNER TO doadmin;

--
-- Name: formatted_rp_entities; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.formatted_rp_entities AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN ((pd.finishing_position)::text ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace((pd.sire_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace((pd.dam_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.id AS course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (rp_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_id)::text)))
  WHERE (pd.race_timestamp > (now() - '5 years'::interval));


ALTER VIEW rp_raw.formatted_rp_entities OWNER TO doadmin;

--
-- Name: missing_dates; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.missing_dates AS
 SELECT gs.date
   FROM (generate_series(('2005-01-01'::date)::timestamp without time zone, ((now())::date - '3 days'::interval), '1 day'::interval) gs(date)
     LEFT JOIN ( SELECT DISTINCT days_results_links.date
           FROM rp_raw.days_results_links) distinct_dates ON ((gs.date = distinct_dates.date)))
  WHERE ((distinct_dates.date IS NULL) AND (NOT (gs.date = ANY (ARRAY[('2020-04-06'::date)::timestamp without time zone, ('2008-12-25'::date)::timestamp without time zone, ('2013-03-29'::date)::timestamp without time zone, ('2010-01-06'::date)::timestamp without time zone, ('2008-12-24'::date)::timestamp without time zone, ('2009-02-02'::date)::timestamp without time zone, ('2007-12-25'::date)::timestamp without time zone, ('2007-12-24'::date)::timestamp without time zone, ('2005-12-25'::date)::timestamp without time zone, ('2005-12-24'::date)::timestamp without time zone, ('2009-12-25'::date)::timestamp without time zone, ('2009-12-24'::date)::timestamp without time zone, ('2006-12-25'::date)::timestamp without time zone, ('2006-12-24'::date)::timestamp without time zone, ('2020-04-03'::date)::timestamp without time zone, ('2020-04-02'::date)::timestamp without time zone, ('2015-12-25'::date)::timestamp without time zone, ('2015-12-24'::date)::timestamp without time zone, ('2016-12-25'::date)::timestamp without time zone, ('2016-12-24'::date)::timestamp without time zone, ('2017-12-25'::date)::timestamp without time zone, ('2017-12-24'::date)::timestamp without time zone, ('2018-12-25'::date)::timestamp without time zone, ('2018-12-24'::date)::timestamp without time zone, ('2019-12-25'::date)::timestamp without time zone, ('2019-12-24'::date)::timestamp without time zone, ('2020-12-25'::date)::timestamp without time zone, ('2020-12-24'::date)::timestamp without time zone, ('2021-12-25'::date)::timestamp without time zone, ('2021-12-24'::date)::timestamp without time zone, ('2022-12-25'::date)::timestamp without time zone, ('2022-12-24'::date)::timestamp without time zone, ('2023-12-25'::date)::timestamp without time zone, ('2023-12-24'::date)::timestamp without time zone]))))
  ORDER BY gs.date DESC;


ALTER VIEW rp_raw.missing_dates OWNER TO doadmin;

--
-- Name: missing_links; Type: VIEW; Schema: rp_raw; Owner: doadmin
--

CREATE VIEW rp_raw.missing_links AS
 WITH courses AS (
         SELECT dl.date,
            dl.link,
            split_part(split_part(dl.link, '://'::text, 2), '/'::text, 4) AS course_part
           FROM rp_raw.days_results_links dl
        )
 SELECT DISTINCT cr.link,
    cr.date
   FROM (courses cr
     LEFT JOIN rp_raw.performance_data pd ON ((cr.link = pd.debug_link)))
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2005-01-01'::date) AND (cr.course_part = ANY (ARRAY['aintree'::text, 'ascot'::text, 'ayr'::text, 'ballinrobe'::text, 'bangor-on-dee'::text, 'bath'::text, 'bellewstown'::text, 'beverley'::text, 'brighton'::text, 'carlisle'::text, 'cartmel'::text, 'catterick'::text, 'chelmsford-aw'::text, 'cheltenham'::text, 'chepstow'::text, 'chester'::text, 'clonmel'::text, 'cork'::text, 'curragh'::text, 'doncaster'::text, 'down-royal'::text, 'downpatrick'::text, 'dundalk-aw'::text, 'epsom'::text, 'exeter'::text, 'fairyhouse'::text, 'fakenham'::text, 'ffos-las'::text, 'fontwell'::text, 'galway'::text, 'goodwood'::text, 'gowran-park'::text, 'hamilton'::text, 'haydock'::text, 'hereford'::text, 'hexham'::text, 'huntingdon'::text, 'kelso'::text, 'kempton'::text, 'kempton-aw'::text, 'kilbeggan'::text, 'killarney'::text, 'laytown'::text, 'leicester'::text, 'leopardstown'::text, 'limerick'::text, 'lingfield'::text, 'lingfield-aw'::text, 'listowel'::text, 'ludlow'::text, 'market-rasen'::text, 'musselburgh'::text, 'naas'::text, 'navan'::text, 'newbury'::text, 'newcastle'::text, 'newcastle-aw'::text, 'newmarket'::text, 'newmarket-july'::text, 'newton-abbot'::text, 'nottingham'::text, 'perth'::text, 'plumpton'::text, 'pontefract'::text, 'punchestown'::text, 'redcar'::text, 'ripon'::text, 'roscommon'::text, 'salisbury'::text, 'sandown'::text, 'sedgefield'::text, 'sligo'::text, 'southwell'::text, 'southwell-aw'::text, 'stratford'::text, 'taunton'::text, 'thirsk'::text, 'thurles'::text, 'tipperary'::text, 'towcester'::text, 'tramore'::text, 'uttoxeter'::text, 'warwick'::text, 'wetherby'::text, 'wexford'::text, 'wexford-rh'::text, 'wincanton'::text, 'windsor'::text, 'wolverhampton-aw'::text, 'worcester'::text, 'yarmouth'::text, 'york'::text])) AND (cr.link <> ALL (ARRAY['https://www.racingpost.com/results/87/wetherby/2011-10-12/539792'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2007-12-10/444939'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/101/worcester/2023-05-26/839708'::text, 'https://www.racingpost.com/results/15/doncaster/2009-12-12/494914'::text, 'https://www.racingpost.com/results/83/towcester/2011-03-17/525075'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2022-03-04/804298'::text, 'https://www.racingpost.com/results/25/hexham/2021-05-01/781541'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2010-10-03/515581'::text, 'https://www.racingpost.com/results/17/epsom/2014-08-25/608198'::text, 'https://www.racingpost.com/results/57/sedgefield/2014-11-25/613650'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2017-02-23/668139'::text, 'https://www.racingpost.com/results/41/perth/2017-09-11/682957'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2020-03-20/754104'::text, 'https://www.racingpost.com/results/20/fontwell/2016-09-30/658390'::text, 'https://www.racingpost.com/results/9/cartmel/2018-05-30/701255'::text, 'https://www.racingpost.com/results/34/ludlow/2014-05-11/600195'::text, 'https://www.racingpost.com/results/8/carlisle/2011-02-21/523611'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/90/wincanton/2016-12-26/664813'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/44/plumpton/2021-03-01/777465'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2018-11-13/714479'::text, 'https://www.racingpost.com/results/54/sandown/2019-12-07/744492'::text, 'https://www.racingpost.com/results/4/bangor-on-dee/2022-10-25/822558'::text, 'https://www.racingpost.com/results/67/stratford/2023-06-20/842207'::text, 'https://www.racingpost.com/results/23/haydock/2018-12-30/717697'::text, 'https://www.racingpost.com/results/44/plumpton/2022-04-18/807343'::text, 'https://www.racingpost.com/results/37/newcastle/2024-03-05/860189'::text])));


ALTER VIEW rp_raw.missing_links OWNER TO doadmin;

--
-- Name: dam; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.dam (
    rp_id text,
    name text,
    tf_id text
);


ALTER TABLE staging.dam OWNER TO doadmin;

--
-- Name: horse; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.horse (
    rp_id text,
    name text,
    tf_id text
);


ALTER TABLE staging.horse OWNER TO doadmin;

--
-- Name: jockey; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.jockey (
    rp_id text,
    name text,
    tf_id text
);


ALTER TABLE staging.jockey OWNER TO doadmin;

--
-- Name: missing_performance_data_vw; Type: VIEW; Schema: staging; Owner: doadmin
--

CREATE VIEW staging.missing_performance_data_vw AS
 SELECT DISTINCT ON (rp.unique_id) rp.horse_id,
    rp.horse_name,
    rp.horse_age,
    rp.jockey_id,
    rp.jockey_name,
    rp.jockey_claim,
    rp.trainer_id,
    rp.trainer_name,
    rp.owner_id,
    rp.owner_name,
    rp.horse_weight,
    rp.or_value,
    rp.finishing_position,
    rp.draw,
    rp.ts_value,
    rp.rpr_value,
    rp.horse_price,
    rp.extra_weight,
    rp.headgear,
    rp.comment,
    rp.sire_name,
    rp.sire_id,
    rp.dam_name,
    rp.dam_id,
    rp.dams_sire,
    rp.dams_sire_id,
    rp.race_date,
    rp.race_title,
    rp.race_time,
    rp.race_timestamp,
    rp.conditions,
    rp.distance,
    rp.going,
    rp.winning_time,
    rp.number_of_runners,
    rp.total_prize_money,
    rp.first_place_prize_money,
    rp.currency,
    rp.course_id,
    rp.course_name,
    rp.course,
    rp.race_id,
    rp.country,
    rp.surface,
    rp.unique_id,
    rp.meeting_id
   FROM (rp_raw.performance_data rp
     LEFT JOIN staging.joined_performance_data sjpd ON (((rp.unique_id)::text = (sjpd.unique_id)::text)))
  WHERE ((sjpd.unique_id IS NULL) AND (rp.race_timestamp <> '2018-12-02 15:05:00'::timestamp without time zone) AND (rp.race_timestamp > '2010-01-01 00:00:00'::timestamp without time zone));


ALTER VIEW staging.missing_performance_data_vw OWNER TO doadmin;

--
-- Name: sire; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.sire (
    rp_id text,
    name text,
    tf_id text
);


ALTER TABLE staging.sire OWNER TO doadmin;

--
-- Name: trainer; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.trainer (
    rp_id text,
    name text,
    tf_id text
);


ALTER TABLE staging.trainer OWNER TO doadmin;

--
-- Name: transformed_performance_data; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.transformed_performance_data (
    race_time timestamp without time zone,
    race_date date,
    horse_name character varying(132),
    age integer,
    draw integer,
    headgear character varying(64),
    weight_carried_st_lbs character varying(16),
    weight_carried_lbs smallint,
    extra_weight_lbs smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    industry_sp character varying(16),
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    official_rating smallint,
    ts smallint,
    rpr smallint,
    tfr smallint,
    tfig smallint,
    in_play_high numeric(6,2),
    in_play_low numeric(6,2),
    in_race_comment text,
    tf_comment text,
    tf_rating_view character varying(16),
    course_id smallint,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    created_at timestamp without time zone
);


ALTER TABLE staging.transformed_performance_data OWNER TO doadmin;

--
-- Name: days_results_links; Type: TABLE; Schema: tf_raw; Owner: doadmin
--

CREATE TABLE tf_raw.days_results_links (
    date date,
    link text
);


ALTER TABLE tf_raw.days_results_links OWNER TO doadmin;

--
-- Name: formatted_tf_entities; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.formatted_tf_entities AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN ((pd.finishing_position)::text ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace((pd.sire_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace((pd.dam_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.id AS course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_id)::text)))
  WHERE (pd.race_timestamp > (now() - '5 years'::interval));


ALTER VIEW tf_raw.formatted_tf_entities OWNER TO doadmin;

--
-- Name: missing_dates; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.missing_dates AS
 SELECT gs.date
   FROM (generate_series(('2005-01-01'::date)::timestamp without time zone, ((now())::date - '3 days'::interval), '1 day'::interval) gs(date)
     LEFT JOIN ( SELECT DISTINCT days_results_links.date
           FROM tf_raw.days_results_links) distinct_dates ON ((gs.date = distinct_dates.date)))
  WHERE ((distinct_dates.date IS NULL) AND (NOT (gs.date = ANY (ARRAY[('2005-03-25'::date)::timestamp without time zone, ('2009-12-23'::date)::timestamp without time zone, ('2020-04-06'::date)::timestamp without time zone, ('2008-12-25'::date)::timestamp without time zone, ('2013-03-29'::date)::timestamp without time zone, ('2010-01-06'::date)::timestamp without time zone, ('2008-12-24'::date)::timestamp without time zone, ('2009-02-02'::date)::timestamp without time zone, ('2007-12-25'::date)::timestamp without time zone, ('2007-12-24'::date)::timestamp without time zone, ('2005-12-25'::date)::timestamp without time zone, ('2005-12-24'::date)::timestamp without time zone, ('2009-12-25'::date)::timestamp without time zone, ('2009-12-24'::date)::timestamp without time zone, ('2006-12-25'::date)::timestamp without time zone, ('2006-12-24'::date)::timestamp without time zone, ('2020-04-03'::date)::timestamp without time zone, ('2020-04-02'::date)::timestamp without time zone, ('2015-12-25'::date)::timestamp without time zone, ('2015-12-24'::date)::timestamp without time zone, ('2016-12-25'::date)::timestamp without time zone, ('2016-12-24'::date)::timestamp without time zone, ('2017-12-25'::date)::timestamp without time zone, ('2017-12-24'::date)::timestamp without time zone, ('2018-12-25'::date)::timestamp without time zone, ('2018-12-24'::date)::timestamp without time zone, ('2019-12-25'::date)::timestamp without time zone, ('2019-12-24'::date)::timestamp without time zone, ('2020-12-25'::date)::timestamp without time zone, ('2020-12-24'::date)::timestamp without time zone, ('2021-12-25'::date)::timestamp without time zone, ('2021-12-24'::date)::timestamp without time zone, ('2022-12-25'::date)::timestamp without time zone, ('2022-12-24'::date)::timestamp without time zone, ('2023-12-25'::date)::timestamp without time zone, ('2023-12-24'::date)::timestamp without time zone]))))
  ORDER BY gs.date DESC;


ALTER VIEW tf_raw.missing_dates OWNER TO doadmin;

--
-- Name: missing_links; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.missing_links AS
 WITH courses AS (
         SELECT dl.date,
            dl.link,
            split_part(split_part(dl.link, '://'::text, 2), '/'::text, 4) AS course_part
           FROM tf_raw.days_results_links dl
        )
 SELECT DISTINCT cr.link,
    cr.date
   FROM (courses cr
     LEFT JOIN tf_raw.performance_data pd ON ((cr.link = pd.debug_link)))
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2005-01-01'::date) AND (cr.course_part = ANY (ARRAY['aintree'::text, 'ascot'::text, 'ayr'::text, 'ballinrobe'::text, 'bangor-on-dee'::text, 'bath'::text, 'bellewstown'::text, 'beverley'::text, 'brighton'::text, 'carlisle'::text, 'cartmel'::text, 'catterick-bridge'::text, 'chelmsford-city'::text, 'cheltenham'::text, 'chepstow'::text, 'chester'::text, 'clonmel'::text, 'cork'::text, 'curragh'::text, 'doncaster'::text, 'down-royal'::text, 'downpatrick'::text, 'dundalk'::text, 'epsom-downs'::text, 'exeter'::text, 'fairyhouse'::text, 'fakenham'::text, 'ffos-las'::text, 'fontwell-park'::text, 'galway'::text, 'goodwood'::text, 'gowran-park'::text, 'hamilton-park'::text, 'haydock-park'::text, 'hereford'::text, 'hexham'::text, 'huntingdon'::text, 'kelso'::text, 'kempton-park'::text, 'kilbeggan'::text, 'killarney'::text, 'laytown'::text, 'leicester'::text, 'leopardstown'::text, 'limerick'::text, 'lingfield-park'::text, 'listowel'::text, 'ludlow'::text, 'market-rasen'::text, 'musselburgh'::text, 'naas'::text, 'navan'::text, 'newbury'::text, 'newcastle'::text, 'newmarket'::text, 'newton-abbot'::text, 'nottingham'::text, 'perth'::text, 'plumpton'::text, 'pontefract'::text, 'punchestown'::text, 'redcar'::text, 'ripon'::text, 'roscommon'::text, 'salisbury'::text, 'sandown'::text, 'sandown-park'::text, 'sedgefield'::text, 'sligo'::text, 'southwell'::text, 'stratford-on-avon'::text, 'taunton'::text, 'thirsk'::text, 'thurles'::text, 'tipperary'::text, 'towcester'::text, 'tramore'::text, 'uttoxeter'::text, 'warwick'::text, 'wetherby'::text, 'wexford'::text, 'wincanton'::text, 'windsor'::text, 'wolverhampton'::text, 'worcester'::text, 'yarmouth'::text, 'york'::text])));


ALTER VIEW tf_raw.missing_links OWNER TO doadmin;

--
-- Name: dam id; Type: DEFAULT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.dam ALTER COLUMN id SET DEFAULT nextval('public.dam_id_seq'::regclass);


--
-- Name: horse id; Type: DEFAULT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.horse ALTER COLUMN id SET DEFAULT nextval('public.horse_id_seq'::regclass);


--
-- Name: jockey id; Type: DEFAULT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.jockey ALTER COLUMN id SET DEFAULT nextval('public.jockey_id_seq'::regclass);


--
-- Name: owner id; Type: DEFAULT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.owner ALTER COLUMN id SET DEFAULT nextval('public.owner_id_seq'::regclass);


--
-- Name: sire id; Type: DEFAULT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.sire ALTER COLUMN id SET DEFAULT nextval('public.sire_id_seq'::regclass);


--
-- Name: trainer id; Type: DEFAULT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.trainer ALTER COLUMN id SET DEFAULT nextval('public.trainer_id_seq'::regclass);


--
-- Name: course course_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.course
    ADD CONSTRAINT course_pkey PRIMARY KEY (id);


--
-- Name: dam dam_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.dam
    ADD CONSTRAINT dam_pkey PRIMARY KEY (id);


--
-- Name: horse horse_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.horse
    ADD CONSTRAINT horse_pkey PRIMARY KEY (id);


--
-- Name: jockey jockey_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.jockey
    ADD CONSTRAINT jockey_pkey PRIMARY KEY (id);


--
-- Name: owner owner_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.owner
    ADD CONSTRAINT owner_pkey PRIMARY KEY (id);


--
-- Name: race race_id_unq; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.race
    ADD CONSTRAINT race_id_unq UNIQUE (race_id);


--
-- Name: dam rp_tf_dam_unique_id; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.dam
    ADD CONSTRAINT rp_tf_dam_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: horse rp_tf_horse_unique_id; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.horse
    ADD CONSTRAINT rp_tf_horse_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: jockey rp_tf_jockey_unique_id; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.jockey
    ADD CONSTRAINT rp_tf_jockey_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: owner rp_tf_owner_unique_id; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.owner
    ADD CONSTRAINT rp_tf_owner_unique_id UNIQUE (rp_id);


--
-- Name: sire rp_tf_sire_unique_id; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.sire
    ADD CONSTRAINT rp_tf_sire_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: sire rp_tf_sire_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.sire
    ADD CONSTRAINT rp_tf_sire_unique_id_v2 UNIQUE (rp_id, tf_id);


--
-- Name: trainer rp_tf_trainer_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.trainer
    ADD CONSTRAINT rp_tf_trainer_unique_id_v2 UNIQUE (rp_id, tf_id);


--
-- Name: sire sire_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.sire
    ADD CONSTRAINT sire_pkey PRIMARY KEY (id);


--
-- Name: trainer trainer_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.trainer
    ADD CONSTRAINT trainer_pkey PRIMARY KEY (id);


--
-- Name: performance_data unique_id_pd; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.performance_data
    ADD CONSTRAINT unique_id_pd UNIQUE (unique_id);


--
-- Name: transformed_race_data race_id_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: doadmin
--

ALTER TABLE ONLY staging.transformed_race_data
    ADD CONSTRAINT race_id_stg_tns UNIQUE (race_id);


--
-- Name: transformed_performance_data unique_id_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: doadmin
--

ALTER TABLE ONLY staging.transformed_performance_data
    ADD CONSTRAINT unique_id_stg_tns UNIQUE (unique_id);


--
-- Name: idx_course_rp_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_course_rp_course_id ON public.course USING btree (rp_id);


--
-- Name: idx_course_tf_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_course_tf_course_id ON public.course USING btree (tf_id);


--
-- Name: idx_dam_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_dam_id ON public.dam USING btree (rp_id);


--
-- Name: idx_dam_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_dam_name ON public.dam USING btree (name);


--
-- Name: idx_horse_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_horse_id ON public.horse USING btree (rp_id);


--
-- Name: idx_horse_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_horse_name ON public.horse USING btree (name);


--
-- Name: idx_jockey_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_jockey_id ON public.jockey USING btree (rp_id);


--
-- Name: idx_jockey_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_jockey_name ON public.jockey USING btree (name);


--
-- Name: idx_performance_data_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_course_id ON public.performance_data USING btree (course_id);


--
-- Name: idx_performance_data_dam_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_dam_id ON public.performance_data USING btree (dam_id);


--
-- Name: idx_performance_data_horse_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_horse_id ON public.performance_data USING btree (horse_id);


--
-- Name: idx_performance_data_jockey_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_jockey_id ON public.performance_data USING btree (jockey_id);


--
-- Name: idx_performance_data_owner_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_owner_id ON public.performance_data USING btree (owner_id);


--
-- Name: idx_performance_data_race_date; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_race_date ON public.performance_data USING btree (race_date);


--
-- Name: idx_performance_data_sire_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_sire_id ON public.performance_data USING btree (sire_id);


--
-- Name: idx_performance_data_trainer_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_trainer_id ON public.performance_data USING btree (trainer_id);


--
-- Name: idx_performance_data_unique_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_performance_data_unique_id ON public.performance_data USING btree (unique_id);


--
-- Name: idx_race_data_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_data_course_id ON public.race USING btree (course_id);


--
-- Name: idx_race_data_meeting_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_data_meeting_id ON public.race USING btree (meeting_id);


--
-- Name: idx_race_data_race_date; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_data_race_date ON public.race USING btree (race_date);


--
-- Name: idx_race_data_race_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_data_race_id ON public.race USING btree (race_id);


--
-- Name: idx_sire_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_sire_id ON public.sire USING btree (rp_id);


--
-- Name: idx_sire_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_sire_name ON public.sire USING btree (name);


--
-- Name: idx_tf_dam_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_tf_dam_id ON public.dam USING btree (tf_id);


--
-- Name: idx_tf_horse_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_tf_horse_id ON public.horse USING btree (tf_id);


--
-- Name: idx_tf_jockey_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_tf_jockey_id ON public.jockey USING btree (tf_id);


--
-- Name: idx_tf_sire_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_tf_sire_id ON public.sire USING btree (tf_id);


--
-- Name: idx_tf_trainer_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_tf_trainer_id ON public.trainer USING btree (tf_id);


--
-- Name: idx_trainer_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_trainer_id ON public.trainer USING btree (rp_id);


--
-- Name: idx_trainer_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_trainer_name ON public.trainer USING btree (name);


--
-- Name: idx_rp_raw_performance_data_course_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_course_id ON rp_raw.performance_data USING btree (course_id);


--
-- Name: idx_rp_raw_performance_data_dam_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_dam_id ON rp_raw.performance_data USING btree (dam_id);


--
-- Name: idx_rp_raw_performance_data_horse_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_horse_id ON rp_raw.performance_data USING btree (horse_id);


--
-- Name: idx_rp_raw_performance_data_jockey_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_jockey_id ON rp_raw.performance_data USING btree (jockey_id);


--
-- Name: idx_rp_raw_performance_data_race_date_course_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_race_date_course_id ON rp_raw.performance_data USING btree (race_date, course_id);


--
-- Name: idx_rp_raw_performance_data_sire_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_sire_id ON rp_raw.performance_data USING btree (sire_id);


--
-- Name: idx_rp_raw_performance_data_trainer_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_trainer_id ON rp_raw.performance_data USING btree (trainer_id);


--
-- Name: idx_rp_raw_performance_data_unique_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_unique_id ON rp_raw.performance_data USING btree (unique_id);


--
-- Name: idx_tf_raw_performance_data_dam_id; Type: INDEX; Schema: tf_raw; Owner: doadmin
--

CREATE INDEX idx_tf_raw_performance_data_dam_id ON tf_raw.performance_data USING btree (dam_id);


--
-- Name: idx_tf_raw_performance_data_horse_id; Type: INDEX; Schema: tf_raw; Owner: doadmin
--

CREATE INDEX idx_tf_raw_performance_data_horse_id ON tf_raw.performance_data USING btree (horse_id);


--
-- Name: idx_tf_raw_performance_data_jockey_id; Type: INDEX; Schema: tf_raw; Owner: doadmin
--

CREATE INDEX idx_tf_raw_performance_data_jockey_id ON tf_raw.performance_data USING btree (jockey_id);


--
-- Name: idx_tf_raw_performance_data_race_date; Type: INDEX; Schema: tf_raw; Owner: doadmin
--

CREATE INDEX idx_tf_raw_performance_data_race_date ON tf_raw.performance_data USING btree (race_date);


--
-- Name: idx_tf_raw_performance_data_race_date_course_id; Type: INDEX; Schema: tf_raw; Owner: doadmin
--

CREATE INDEX idx_tf_raw_performance_data_race_date_course_id ON tf_raw.performance_data USING btree (race_date, course_id);


--
-- Name: idx_tf_raw_performance_data_sire_id; Type: INDEX; Schema: tf_raw; Owner: doadmin
--

CREATE INDEX idx_tf_raw_performance_data_sire_id ON tf_raw.performance_data USING btree (sire_id);


--
-- Name: idx_tf_raw_performance_data_trainer_id; Type: INDEX; Schema: tf_raw; Owner: doadmin
--

CREATE INDEX idx_tf_raw_performance_data_trainer_id ON tf_raw.performance_data USING btree (trainer_id);


--
-- PostgreSQL database dump complete
--

