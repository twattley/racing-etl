--
-- PostgreSQL database dump
--

-- Dumped from database version 16.4 (Postgres.app)
-- Dumped by pg_dump version 16.4 (Postgres.app)

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
-- Name: backup; Type: SCHEMA; Schema: -; Owner: tomwattley
--

CREATE SCHEMA backup;


ALTER SCHEMA backup OWNER TO tomwattley;

--
-- Name: betting; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA betting;


ALTER SCHEMA betting OWNER TO postgres;

--
-- Name: bf_raw; Type: SCHEMA; Schema: -; Owner: tomwattley
--

CREATE SCHEMA bf_raw;


ALTER SCHEMA bf_raw OWNER TO tomwattley;

--
-- Name: errors; Type: SCHEMA; Schema: -; Owner: tomwattley
--

CREATE SCHEMA errors;


ALTER SCHEMA errors OWNER TO tomwattley;

--
-- Name: metrics; Type: SCHEMA; Schema: -; Owner: tomwattley
--

CREATE SCHEMA metrics;


ALTER SCHEMA metrics OWNER TO tomwattley;

--
-- Name: rp_raw; Type: SCHEMA; Schema: -; Owner: tomwattley
--

CREATE SCHEMA rp_raw;


ALTER SCHEMA rp_raw OWNER TO tomwattley;

--
-- Name: staging; Type: SCHEMA; Schema: -; Owner: tomwattley
--

CREATE SCHEMA staging;


ALTER SCHEMA staging OWNER TO tomwattley;

--
-- Name: tf_raw; Type: SCHEMA; Schema: -; Owner: tomwattley
--

CREATE SCHEMA tf_raw;


ALTER SCHEMA tf_raw OWNER TO tomwattley;

--
-- Name: update_selections_info(); Type: PROCEDURE; Schema: betting; Owner: postgres
--

CREATE PROCEDURE betting.update_selections_info()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO betting.selections_info (
        betting_type, session_id, horse_name, age, horse_sex, draw, headgear, weight_carried,
        weight_carried_lbs, extra_weight, jockey_claim, finishing_position,
        total_distance_beaten, industry_sp, betfair_win_sp, betfair_place_sp,
        official_rating, ts, rpr, tfr, tfig, in_play_high, in_play_low,
        in_race_comment, tf_comment, tfr_view, race_id, horse_id, jockey_id,
        trainer_id, owner_id, sire_id, dam_id, unique_id, race_time, race_date,
        race_title, race_type, race_class, distance, distance_yards,
        distance_meters, distance_kilometers, conditions, going,
        number_of_runners, hcap_range, age_range, surface, total_prize_money,
        first_place_prize_money, winning_time, time_seconds, relative_time,
        relative_to_standard, country, main_race_comment, meeting_id, course_id,
        course, dam, sire, trainer, jockey, price_move, created_at
    )
    WITH latest_bets AS (
        SELECT * 
        FROM betting.selections 
        WHERE created_at > COALESCE(
            (SELECT MAX(created_at) FROM betting.selections_info),
            '2000-01-01'::date
        )
    )
    SELECT 
        lb.betting_type, 
		lb.session_id,
        pd.horse_name,
        pd.age,
        pd.horse_sex,
        pd.draw,
        pd.headgear,
        pd.weight_carried,
        pd.weight_carried_lbs,
        pd.extra_weight,
        pd.jockey_claim,
        pd.finishing_position,
        pd.total_distance_beaten,
        pd.industry_sp,
        pd.betfair_win_sp,
        pd.betfair_place_sp,
        pd.official_rating,
        pd.ts,
        pd.rpr,
        pd.tfr,
        pd.tfig,
        pd.in_play_high,
        pd.in_play_low,
        pd.in_race_comment,
        pd.tf_comment,
        pd.tfr_view,
        pd.race_id,
        pd.horse_id,
        pd.jockey_id,
        pd.trainer_id,
        pd.owner_id,
        pd.sire_id,
        pd.dam_id,
        pd.unique_id,
        pd.race_time,
        pd.race_date,
        pd.race_title,
        pd.race_type,
        pd.race_class,
        pd.distance,
        pd.distance_yards,
        pd.distance_meters,
        pd.distance_kilometers,
        pd.conditions,
        pd.going,
        pd.number_of_runners,
        pd.hcap_range,
        pd.age_range,
        pd.surface,
        pd.total_prize_money,
        pd.first_place_prize_money,
        pd.winning_time,
        pd.time_seconds,
        pd.relative_time,
        pd.relative_to_standard,
        pd.country,
        pd.main_race_comment,
        pd.meeting_id,
        pd.course_id,
        pd.course,
        pd.dam,
        pd.sire,
        pd.trainer,
        pd.jockey,
        pd.price_move,
        now() as created_at 
    FROM latest_bets lb
    LEFT JOIN public.unioned_performance_data pd 
    ON pd.race_id = lb.race_id 
    AND pd.horse_id = lb.horse_id
    AND pd.race_date = lb.race_date
    ON CONFLICT (unique_id) 
    DO UPDATE SET
        horse_name = EXCLUDED.horse_name,
        age = EXCLUDED.age,
        horse_sex = EXCLUDED.horse_sex,
        draw = EXCLUDED.draw,
        headgear = EXCLUDED.headgear,
        weight_carried = EXCLUDED.weight_carried,
        weight_carried_lbs = EXCLUDED.weight_carried_lbs,
        extra_weight = EXCLUDED.extra_weight,
        jockey_claim = EXCLUDED.jockey_claim,
        finishing_position = EXCLUDED.finishing_position,
        total_distance_beaten = EXCLUDED.total_distance_beaten,
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
        tfr_view = EXCLUDED.tfr_view,
        race_id = EXCLUDED.race_id,
        horse_id = EXCLUDED.horse_id,
        jockey_id = EXCLUDED.jockey_id,
        trainer_id = EXCLUDED.trainer_id,
        owner_id = EXCLUDED.owner_id,
        sire_id = EXCLUDED.sire_id,
        dam_id = EXCLUDED.dam_id,
        race_time = EXCLUDED.race_time,
        race_date = EXCLUDED.race_date,
        race_title = EXCLUDED.race_title,
        race_type = EXCLUDED.race_type,
        race_class = EXCLUDED.race_class,
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
        total_prize_money = EXCLUDED.total_prize_money,
        first_place_prize_money = EXCLUDED.first_place_prize_money,
        winning_time = EXCLUDED.winning_time,
        time_seconds = EXCLUDED.time_seconds,
        relative_time = EXCLUDED.relative_time,
        relative_to_standard = EXCLUDED.relative_to_standard,
        country = EXCLUDED.country,
        main_race_comment = EXCLUDED.main_race_comment,
        meeting_id = EXCLUDED.meeting_id,
        course_id = EXCLUDED.course_id,
        course = EXCLUDED.course,
        dam = EXCLUDED.dam,
        sire = EXCLUDED.sire,
        trainer = EXCLUDED.trainer,
        jockey = EXCLUDED.jockey,
        price_move = EXCLUDED.price_move,
        created_at = EXCLUDED.created_at;
END;
$$;


ALTER PROCEDURE betting.update_selections_info() OWNER TO postgres;

--
-- Name: update_historical_price_data(); Type: PROCEDURE; Schema: bf_raw; Owner: postgres
--

CREATE PROCEDURE bf_raw.update_historical_price_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.historical_price_data (
        race_time,
        race_date,
        horse_name,
        horse_id,
        course_id,
        meeting_id,
        race_id,
        runner_id,
        race_key,
        unique_id,
        bf_unique_id,
        price_change
    )
    SELECT DISTINCT ON (pd.unique_id, hpd.bf_unique_id)
        pd.race_timestamp as race_time,
        pd.race_date,
        pd.horse_name,
        pd.horse_id,
        pd.course_id,
        pd.meeting_id,
        pd.race_id,
        hpd.runner_id,
        hpd.race_key,
        pd.unique_id,
        hpd.bf_unique_id,
        hpd.price_change
    FROM 
        rp_raw.performance_data pd
    LEFT JOIN 
        public.course c ON pd.course_id = c.rp_id
    INNER JOIN 
        bf_raw.historical_price_data hpd ON 
        pd.horse_name = hpd.horse AND
        c.bf_name = hpd.course AND
        pd.race_date::date = hpd.race_date::date
    WHERE 
        NOT EXISTS (
            SELECT 1
            FROM public.historical_price_data hpd_existing
            WHERE hpd_existing.unique_id = pd.unique_id
            AND hpd_existing.bf_unique_id = hpd.bf_unique_id
        )
    ORDER BY pd.unique_id, hpd.bf_unique_id, pd.race_timestamp DESC
    ON CONFLICT (unique_id, bf_unique_id) DO NOTHING;

    COMMIT;
END;
$$;


ALTER PROCEDURE bf_raw.update_historical_price_data() OWNER TO postgres;

--
-- Name: update_horse_betfair_data(); Type: PROCEDURE; Schema: bf_raw; Owner: postgres
--

CREATE PROCEDURE bf_raw.update_horse_betfair_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Update horses where bf_id and bf_name are null
    UPDATE horse h
    SET 
        bf_id = subquery.horse_id
    FROM (
        SELECT DISTINCT ON (h.id) h.id, tprd.horse_id, tprd.horse_name
        FROM rp_raw.todays_performance_data tpd
        JOIN horse h ON tpd.horse_id = h.rp_id
        JOIN course c ON tpd.course_id = c.rp_id
        JOIN bf_raw.todays_price_data tprd
            ON tprd.course = c.bf_name
            AND tprd.horse_name = tpd.horse_name
            AND tprd.race_time = tpd.race_timestamp
        WHERE tprd.horse_id IS NOT NULL
          AND h.bf_id IS NULL
    ) AS subquery
    WHERE h.id = subquery.id;
END;
$$;


ALTER PROCEDURE bf_raw.update_horse_betfair_data() OWNER TO postgres;

--
-- Name: update_horse_betfair_data_race_time(); Type: PROCEDURE; Schema: bf_raw; Owner: postgres
--

CREATE PROCEDURE bf_raw.update_horse_betfair_data_race_time()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Update existing horses
    UPDATE horse h
    SET 
        bf_id = subquery.horse_id,
        bf_name = subquery.horse_name
    FROM (
        SELECT DISTINCT ON (h.id) h.id, tprd.horse_id, tprd.horse_name
        FROM rp_raw.todays_performance_data tpd
        LEFT JOIN horse h ON tpd.horse_id = h.rp_id
        LEFT JOIN course c ON tpd.course_id = c.rp_id
        LEFT JOIN bf_raw.todays_price_data tprd
            ON tprd.course = c.bf_name
            AND tprd.horse_name = tpd.horse_name
            AND tprd.race_time = tpd.race_timestamp
        WHERE tprd.horse_id IS NOT NULL
    ) AS subquery
    WHERE h.id = subquery.id
    AND (h.bf_id IS DISTINCT FROM subquery.horse_id OR h.bf_name IS DISTINCT FROM subquery.horse_name);

    -- Log the number of updated rows
    RAISE NOTICE 'Updated % rows in the horse table', ROW_COUNT();
END;
$$;


ALTER PROCEDURE bf_raw.update_horse_betfair_data_race_time() OWNER TO postgres;

--
-- Name: update_matched_historical_price_data(); Type: PROCEDURE; Schema: bf_raw; Owner: postgres
--

CREATE PROCEDURE bf_raw.update_matched_historical_price_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.historical_price_data (
        race_time,
        race_date,
        horse_name,
        horse_id,
        course_id,
        meeting_id,
        race_id,
        runner_id,
        race_key,
        unique_id,
        bf_unique_id,
        price_change
    )
    SELECT DISTINCT ON (unique_id, bf_unique_id)
        race_time,
        race_date,
        horse_name,
        horse_id,
        course_id,
        meeting_id,
        race_id,
        runner_id,
        race_key,
        unique_id,
        bf_unique_id,
        price_change
    FROM 
        bf_raw.matched_historical_price_data
    ON CONFLICT (unique_id, bf_unique_id) DO NOTHING;

END;
$$;


ALTER PROCEDURE bf_raw.update_matched_historical_price_data() OWNER TO postgres;

--
-- Name: get_day_name(date); Type: FUNCTION; Schema: errors; Owner: tomwattley
--

CREATE FUNCTION errors.get_day_name(input_date date) RETURNS text
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Returns the name of the day for the given date
    RETURN TO_CHAR(input_date, 'Day');
END;
$$;


ALTER FUNCTION errors.get_day_name(input_date date) OWNER TO tomwattley;

--
-- Name: refresh_unmatched_rp_dams(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_rp_dams()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.rp_unmatched_dams;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.rp_unmatched_dams
    SELECT dam_name, dam_id, race_timestamp, course_id, unique_id, race_date
	FROM rp_raw.unmatched_dams;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_rp_dams() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_rp_horses(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_rp_horses()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.rp_unmatched_horses;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.rp_unmatched_horses
    SELECT horse_name, horse_id, race_timestamp, course_id, unique_id, race_date
	FROM rp_raw.unmatched_horses;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_rp_horses() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_rp_jockeys(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_rp_jockeys()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.rp_unmatched_jockeys;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.rp_unmatched_jockeys
    SELECT jockey_name, jockey_id, race_timestamp, course_id, unique_id, race_date
	FROM rp_raw.unmatched_jockeys;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_rp_jockeys() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_rp_sires(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_rp_sires()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.rp_unmatched_sires;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.rp_unmatched_sires
    SELECT sire_name, sire_id, race_timestamp, course_id, unique_id, race_date
	FROM rp_raw.unmatched_sires;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_rp_sires() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_rp_trainers(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_rp_trainers()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.rp_unmatched_trainers;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.rp_unmatched_trainers
    SELECT trainer_name, trainer_id, race_timestamp, course_id, unique_id, race_date
	FROM rp_raw.unmatched_trainers;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_rp_trainers() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_tf_dams(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_tf_dams()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.tf_unmatched_dams;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.tf_unmatched_dams
    SELECT dam_name, dam_id, race_timestamp, course_id, unique_id, race_date
	FROM tf_raw.unmatched_dams;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_tf_dams() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_tf_horses(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_tf_horses()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.tf_unmatched_horses;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.tf_unmatched_horses
    SELECT horse_name, horse_id, race_timestamp, course_id, unique_id, race_date 
	FROM tf_raw.unmatched_horses
	order by race_timestamp;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_tf_horses() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_tf_jockeys(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_tf_jockeys()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.tf_unmatched_jockeys;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.tf_unmatched_jockeys
    SELECT jockey_name, jockey_id, race_timestamp, course_id, unique_id, race_date
	FROM tf_raw.unmatched_jockeys;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_tf_jockeys() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_tf_sires(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_tf_sires()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.tf_unmatched_sires;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.tf_unmatched_sires
    SELECT sire_name, sire_id, race_timestamp, course_id, unique_id, race_date
	FROM tf_raw.unmatched_sires;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_tf_sires() OWNER TO tomwattley;

--
-- Name: refresh_unmatched_tf_trainers(); Type: PROCEDURE; Schema: metrics; Owner: tomwattley
--

CREATE PROCEDURE metrics.refresh_unmatched_tf_trainers()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Truncate the table to delete all its data
    TRUNCATE TABLE metrics.tf_unmatched_trainers;
    
    -- Insert data from the view into the table
    INSERT INTO metrics.tf_unmatched_trainers
    SELECT trainer_name, trainer_id, race_timestamp, course_id, unique_id, race_date
	FROM tf_raw.unmatched_trainers;
    
    -- Optional: Add a notice or log output for completion
    RAISE NOTICE 'Data refresh complete.';
END;
$$;


ALTER PROCEDURE metrics.refresh_unmatched_tf_trainers() OWNER TO tomwattley;

--
-- Name: create_todays_performance_data_vw(); Type: PROCEDURE; Schema: public; Owner: tomwattley
--

CREATE PROCEDURE public.create_todays_performance_data_vw()
    LANGUAGE plpgsql
    AS $$
BEGIN

	TRUNCATE public.todays_performance_data_mat_vw;
	
    INSERT INTO public.todays_performance_data_mat_vw
    (
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
		price_change,
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
		data_type,
		created_at
    )
WITH todays_form as (
	SELECT DISTINCT ON (tpd.unique_id)
				tpd.horse_name,
				tpd.age,
				tpd.horse_sex,
				tpd.draw,
				tpd.headgear,
				tpd.weight_carried,
				tpd.weight_carried_lbs,
				tpd.extra_weight,
				tpd.jockey_claim,
				tpd.finishing_position,
				tpd.total_distance_beaten,
				tpd.industry_sp,
				tpd.betfair_win_sp,
				tpd.betfair_place_sp,
                CAST(NULL AS numeric(6,2)) AS price_change,
				tpd.official_rating,
                CAST(NULL AS SMALLINT) as ts,  
                CAST(NULL AS SMALLINT) as rpr, 
                CAST(NULL AS SMALLINT) as tfr, 
                CAST(NULL AS SMALLINT) as tfig, 
				tpd.in_play_high,
				tpd.in_play_low,
				tpd.in_race_comment,
				tpd.tf_comment,
				tpd.tfr_view,
                trd.race_id,
				tpd.horse_id,
				tpd.jockey_id,
				tpd.trainer_id,
				tpd.owner_id,
				tpd.sire_id,
				tpd.dam_id,
				tpd.unique_id,
                trd.race_time,
                trd.race_date,
                trd.race_title,
                trd.race_type,
                trd.race_class,
                trd.distance,
                trd.distance_yards,
                trd.distance_meters,
                trd.distance_kilometers,
                trd.conditions,
                trd.going,
                trd.number_of_runners,
                trd.hcap_range,
                trd.age_range,
                sf.name as surface,
                trd.total_prize_money,
                trd.first_place_prize_money,
                trd.winning_time,
                trd.time_seconds,
                trd.relative_time,
                trd.relative_to_standard,
                trd.country,
                trd.main_race_comment,
                trd.meeting_id,
                trd.course_id,
				c.name as course, 
				d.name as dam, 
				s.name as sire, 
				t.name as trainer, 
				j.name as jockey,
                'today'::character varying AS data_type,
                now() as created_at
	FROM todays_performance_data tpd
	LEFT JOIN todays_race_data trd ON tpd.race_id = trd.race_id
	LEFT JOIN course c ON tpd.course_id = c.id
	LEFT JOIN surface sf ON c.surface_id = sf.id
	LEFT JOIN horse h ON tpd.horse_id = h.id
	LEFT JOIN dam d ON tpd.dam_id = d.id
	LEFT JOIN sire s ON tpd.sire_id = s.id
	LEFT JOIN trainer t ON tpd.trainer_id = t.id
	LEFT JOIN jockey j ON tpd.jockey_id = j.id
		),
			
historical_form as (
	SELECT pd.horse_name,
		pd.age,
		pd.horse_sex,
		pd.draw,
		pd.headgear,
		pd.weight_carried,
		pd.weight_carried_lbs,
		pd.extra_weight,
		pd.jockey_claim,
		pd.finishing_position,
		pd.total_distance_beaten,
		pd.industry_sp,
		pd.betfair_win_sp,
		pd.betfair_place_sp,
		hpd.price_change,
		pd.official_rating,
		pd.ts,  
		pd.rpr, 
		pd.tfr, 
		pd.tfig, 
		pd.in_play_high,
		pd.in_play_low,
		pd.in_race_comment,
		pd.tf_comment,
		pd.tfr_view,
		pd.race_id,
		pd.horse_id,
		pd.jockey_id,
		pd.trainer_id,
		pd.owner_id,
		pd.sire_id,
		pd.dam_id,
		pd.unique_id,
		pd.race_time,
		pd.race_date,
		pd.race_title,
		pd.race_type,
		pd.race_class,
		pd.distance,
		pd.distance_yards,
		pd.distance_meters,
		pd.distance_kilometers,
		pd.conditions,
		pd.going,
		pd.number_of_runners,
		pd.hcap_range,
		pd.age_range,
		pd.surface,
		pd.total_prize_money,
		pd.first_place_prize_money,
		pd.winning_time,
		pd.time_seconds,
		pd.relative_time,
		pd.relative_to_standard,
		pd.country,
		pd.main_race_comment,
		pd.meeting_id,
		pd.course_id,
		pd.course,
		pd.dam,
		pd.sire,
		pd.trainer,
		pd.jockey, 
		'historical'::character varying AS data_type,
		now() as created_at
FROM public.unioned_performance_data pd
		LEFT JOIN public.historical_price_data hpd
		ON pd.unique_id = hpd.unique_id
WHERE pd.horse_id IN (
    SELECT tf.horse_id
    FROM todays_form tf
)
AND pd.race_date < current_date
ORDER BY pd.horse_name, pd.race_time DESC)

SELECT horse_name,
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
		price_change,
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
		data_type,
		created_at
FROM todays_form
 UNION
SELECT horse_name,
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
		price_change,
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
		data_type,
		created_at
FROM historical_form;
END;
$$;


ALTER PROCEDURE public.create_todays_performance_data_vw() OWNER TO tomwattley;

--
-- Name: insert_feedback_data_by_date(date); Type: FUNCTION; Schema: public; Owner: tomwattley
--

CREATE FUNCTION public.insert_feedback_data_by_date(input_race_date date) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    TRUNCATE public.feedback_performance_data_mat_vw;
    
    INSERT INTO public.feedback_performance_data_mat_vw
    (
        horse_name, age, horse_sex, draw, headgear, weight_carried, weight_carried_lbs,
        extra_weight, jockey_claim, finishing_position, total_distance_beaten, industry_sp,
        betfair_win_sp, betfair_place_sp, price_change, official_rating, ts, rpr, tfr, tfig,
        in_play_high, in_play_low, in_race_comment, tf_comment, tfr_view, race_id, horse_id,
        jockey_id, trainer_id, owner_id, sire_id, dam_id, unique_id, race_time, race_date,
        race_title, race_type, race_class, distance, distance_yards, distance_meters,
        distance_kilometers, conditions, going, number_of_runners, hcap_range, age_range,
        surface, total_prize_money, first_place_prize_money, winning_time, time_seconds,
        relative_time, relative_to_standard, country, main_race_comment, meeting_id,
        course_id, course, dam, sire, trainer, jockey, data_type
    )
    WITH todays_form AS (
        SELECT pd.horse_name,
            pd.age,
            pd.horse_sex,
            pd.draw,
            pd.headgear,
            pd.weight_carried,
            pd.weight_carried_lbs,
            pd.extra_weight,
            pd.jockey_claim,
            pd.finishing_position,
            pd.total_distance_beaten,
            pd.industry_sp,
            pd.betfair_win_sp,
            pd.betfair_place_sp,
            hpd.price_change,
            pd.official_rating,
            NULL::smallint AS ts,
            NULL::smallint AS rpr,
            NULL::smallint AS tfr,
            NULL::smallint AS tfig,
            pd.in_play_high,
            pd.in_play_low,
            pd.in_race_comment,
            pd.tf_comment,
            pd.tfr_view,
            pd.race_id,
            pd.horse_id,
            pd.jockey_id,
            pd.trainer_id,
            pd.owner_id,
            pd.sire_id,
            pd.dam_id,
            pd.unique_id,
            pd.race_time,
            pd.race_date,
            pd.race_title,
            pd.race_type,
            pd.race_class,
            pd.distance,
            pd.distance_yards,
            pd.distance_meters,
            pd.distance_kilometers,
            pd.conditions,
            pd.going,
            pd.number_of_runners,
            pd.hcap_range,
            pd.age_range,
            pd.surface,
            pd.total_prize_money,
            pd.first_place_prize_money,
            pd.winning_time,
            pd.time_seconds,
            pd.relative_time,
            pd.relative_to_standard,
            pd.country,
            pd.main_race_comment,
            pd.meeting_id,
            pd.course_id,
            pd.course,
            pd.dam,
            pd.sire,
            pd.trainer,
            pd.jockey,
            'today'::character varying AS data_type
        FROM unioned_performance_data pd
        LEFT JOIN historical_price_data hpd ON pd.unique_id::text = hpd.unique_id::text
        WHERE pd.horse_id IN (
            SELECT pd_1.horse_id
            FROM unioned_performance_data pd_1
            WHERE pd_1.race_date = input_race_date
        ) 
        AND pd.race_date = input_race_date
        ORDER BY pd.horse_name, pd.race_time DESC
    ), historical_form AS (
        SELECT pd.horse_name,
            pd.age,
            pd.horse_sex,
            pd.draw,
            pd.headgear,
            pd.weight_carried,
            pd.weight_carried_lbs,
            pd.extra_weight,
            pd.jockey_claim,
            pd.finishing_position,
            pd.total_distance_beaten,
            pd.industry_sp,
            pd.betfair_win_sp,
            pd.betfair_place_sp,
            hpd.price_change,
            pd.official_rating,
            pd.ts,
            pd.rpr,
            pd.tfr,
            pd.tfig,
            pd.in_play_high,
            pd.in_play_low,
            pd.in_race_comment,
            pd.tf_comment,
            pd.tfr_view,
            pd.race_id,
            pd.horse_id,
            pd.jockey_id,
            pd.trainer_id,
            pd.owner_id,
            pd.sire_id,
            pd.dam_id,
            pd.unique_id,
            pd.race_time,
            pd.race_date,
            pd.race_title,
            pd.race_type,
            pd.race_class,
            pd.distance,
            pd.distance_yards,
            pd.distance_meters,
            pd.distance_kilometers,
            pd.conditions,
            pd.going,
            pd.number_of_runners,
            pd.hcap_range,
            pd.age_range,
            pd.surface,
            pd.total_prize_money,
            pd.first_place_prize_money,
            pd.winning_time,
            pd.time_seconds,
            pd.relative_time,
            pd.relative_to_standard,
            pd.country,
            pd.main_race_comment,
            pd.meeting_id,
            pd.course_id,
            pd.course,
            pd.dam,
            pd.sire,
            pd.trainer,
            pd.jockey,
            'historical'::character varying AS data_type
        FROM unioned_performance_data pd
        LEFT JOIN historical_price_data hpd ON pd.unique_id::text = hpd.unique_id::text
        WHERE pd.horse_id IN (
            SELECT pd_1.horse_id
            FROM unioned_performance_data pd_1
            WHERE pd_1.race_date = input_race_date
        ) AND pd.race_date < input_race_date
        ORDER BY pd.horse_name, pd.race_time DESC
    )
    SELECT * FROM todays_form
    UNION ALL
    SELECT * FROM historical_form;
END;
$$;


ALTER FUNCTION public.insert_feedback_data_by_date(input_race_date date) OWNER TO tomwattley;

--
-- Name: insert_historical_price_data(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.insert_historical_price_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.historical_price_data (
        race_time,
        race_date,
        horse_name,
        horse_id,
        course_id,
        meeting_id,
        race_id,
        runner_id,
        race_key,
        unique_id,
        bf_unique_id,
        price_change
    )
    SELECT DISTINCT ON (pd.unique_id, hpd.bf_unique_id)
        pd.race_timestamp as race_time,
        pd.race_date,
        pd.horse_name,
        pd.horse_id,
        pd.course_id,
        pd.meeting_id,
        pd.race_id,
        hpd.runner_id,
        hpd.race_key,
        pd.unique_id,
        hpd.bf_unique_id,
        hpd.price_change
    FROM 
        rp_raw.performance_data pd
    LEFT JOIN 
        public.course c ON pd.course_id = c.rp_id
    INNER JOIN 
        bf_raw.historical_price_data hpd ON 
        pd.horse_name = hpd.horse AND
        c.bf_name = hpd.course AND
        pd.race_date::date = hpd.race_date::date
    ORDER BY pd.unique_id, hpd.bf_unique_id, pd.race_timestamp DESC
    ON CONFLICT (unique_id, bf_unique_id) DO UPDATE
    SET 
        race_time = EXCLUDED.race_time,
        race_date = EXCLUDED.race_date,
        horse_name = EXCLUDED.horse_name,
        horse_id = EXCLUDED.horse_id,
        course_id = EXCLUDED.course_id,
        meeting_id = EXCLUDED.meeting_id,
        race_id = EXCLUDED.race_id,
        runner_id = EXCLUDED.runner_id,
        race_key = EXCLUDED.race_key,
        price_change = EXCLUDED.price_change;

    COMMIT;
END;
$$;


ALTER PROCEDURE public.insert_historical_price_data() OWNER TO postgres;

--
-- Name: insert_todays_transformed_performance_data(); Type: PROCEDURE; Schema: public; Owner: tomwattley
--

CREATE PROCEDURE public.insert_todays_transformed_performance_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	TRUNCATE public.todays_performance_data;
	
    INSERT INTO public.todays_performance_data
    (
		race_time,
		race_date,
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
		course_id,
		horse_id,
		jockey_id,
		trainer_id,
		owner_id,
		sire_id,
		dam_id,
		unique_id,
		created_at
    )
    SELECT
		race_time,
		race_date,
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
		course_id,
		horse_id,
		jockey_id,
		trainer_id,
		owner_id,
		sire_id,
		dam_id,
		unique_id,
		NOW()
    FROM staging.todays_transformed_performance_data;
END;
$$;


ALTER PROCEDURE public.insert_todays_transformed_performance_data() OWNER TO tomwattley;

--
-- Name: insert_todays_transformed_race_data(); Type: PROCEDURE; Schema: public; Owner: tomwattley
--

CREATE PROCEDURE public.insert_todays_transformed_race_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
	TRUNCATE public.todays_race_data; 
	
    INSERT INTO public.todays_race_data
    (
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
		race_id,
		course_id,
		created_at
    )
    SELECT
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
		race_id,
		course_id,
		NOW()
    FROM staging.todays_transformed_race_data; 
END;
$$;


ALTER PROCEDURE public.insert_todays_transformed_race_data() OWNER TO tomwattley;

--
-- Name: insert_transformed_non_uk_ire_performance_data(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.insert_transformed_non_uk_ire_performance_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.non_uk_ire_performance_data
    (
		race_time,
		race_date,
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
		course_id,
		horse_id,
		jockey_id,
		trainer_id,
		owner_id,
		sire_id,
		dam_id,
		unique_id,
		created_at
    )
    SELECT
		race_time,
		race_date,
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
		course_id,
		horse_id,
		jockey_id,
		trainer_id,
		owner_id,
		sire_id,
		dam_id,
		unique_id,
		NOW()
    FROM staging.transformed_non_uk_ire_performance_data
    ON CONFLICT (unique_id) DO UPDATE SET
		race_time = EXCLUDED.race_time,
		race_date = EXCLUDED.race_date,
		horse_name = EXCLUDED.horse_name,
		age = EXCLUDED.age,
		horse_sex = EXCLUDED.horse_sex,
		draw = EXCLUDED.draw,
		headgear = EXCLUDED.headgear,
		weight_carried = EXCLUDED.weight_carried,
		weight_carried_lbs = EXCLUDED.weight_carried_lbs,
		extra_weight = EXCLUDED.extra_weight,
		jockey_claim = EXCLUDED.jockey_claim,
		finishing_position = EXCLUDED.finishing_position,
		total_distance_beaten = EXCLUDED.total_distance_beaten,
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
		tfr_view = EXCLUDED.tfr_view,
		course_id = EXCLUDED.course_id,
		horse_id = EXCLUDED.horse_id,
		jockey_id = EXCLUDED.jockey_id,
		trainer_id = EXCLUDED.trainer_id,
		owner_id = EXCLUDED.owner_id,
		sire_id = EXCLUDED.sire_id,
		dam_id = EXCLUDED.dam_id,
		unique_id = EXCLUDED.unique_id,
		created_at = EXCLUDED.created_at;
END;
$$;


ALTER PROCEDURE public.insert_transformed_non_uk_ire_performance_data() OWNER TO postgres;

--
-- Name: insert_transformed_non_uk_ire_race_data(); Type: PROCEDURE; Schema: public; Owner: tomwattley
--

CREATE PROCEDURE public.insert_transformed_non_uk_ire_race_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.non_uk_ire_race_data
    (
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
		race_id,
		course_id,
		created_at
    )
    SELECT
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
		race_id,
		course_id,
		NOW()
    FROM staging.transformed_non_uk_ire_race_data
    ON CONFLICT (race_id) DO UPDATE SET
		race_time = EXCLUDED.race_time,
		race_date = EXCLUDED.race_date,
		race_title = EXCLUDED.race_title,
		race_type = EXCLUDED.race_type,
		race_class = EXCLUDED.race_class,
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
		total_prize_money = EXCLUDED.total_prize_money,
		first_place_prize_money = EXCLUDED.first_place_prize_money,
		winning_time = EXCLUDED.winning_time,
		time_seconds = EXCLUDED.time_seconds,
		relative_time = EXCLUDED.relative_time,
		relative_to_standard = EXCLUDED.relative_to_standard,
		country = EXCLUDED.country,
		main_race_comment = EXCLUDED.main_race_comment,
		meeting_id = EXCLUDED.meeting_id,
		race_id = EXCLUDED.race_id,
		course_id = EXCLUDED.course_id,
        created_at = EXCLUDED.created_at; 
END;
$$;


ALTER PROCEDURE public.insert_transformed_non_uk_ire_race_data() OWNER TO tomwattley;

--
-- Name: insert_transformed_performance_data(); Type: PROCEDURE; Schema: public; Owner: tomwattley
--

CREATE PROCEDURE public.insert_transformed_performance_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.performance_data
    (
		race_time,
		race_date,
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
		course_id,
		horse_id,
		jockey_id,
		trainer_id,
		owner_id,
		sire_id,
		dam_id,
		unique_id,
		created_at
    )
    SELECT
		race_time,
		race_date,
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
		course_id,
		horse_id,
		jockey_id,
		trainer_id,
		owner_id,
		sire_id,
		dam_id,
		unique_id,
		NOW()
    FROM staging.transformed_performance_data
    ON CONFLICT (unique_id) DO UPDATE SET
		race_time = EXCLUDED.race_time,
		race_date = EXCLUDED.race_date,
		horse_name = EXCLUDED.horse_name,
		age = EXCLUDED.age,
		horse_sex = EXCLUDED.horse_sex,
		draw = EXCLUDED.draw,
		headgear = EXCLUDED.headgear,
		weight_carried = EXCLUDED.weight_carried,
		weight_carried_lbs = EXCLUDED.weight_carried_lbs,
		extra_weight = EXCLUDED.extra_weight,
		jockey_claim = EXCLUDED.jockey_claim,
		finishing_position = EXCLUDED.finishing_position,
		total_distance_beaten = EXCLUDED.total_distance_beaten,
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
		tfr_view = EXCLUDED.tfr_view,
		course_id = EXCLUDED.course_id,
		horse_id = EXCLUDED.horse_id,
		jockey_id = EXCLUDED.jockey_id,
		trainer_id = EXCLUDED.trainer_id,
		owner_id = EXCLUDED.owner_id,
		sire_id = EXCLUDED.sire_id,
		dam_id = EXCLUDED.dam_id,
		unique_id = EXCLUDED.unique_id,
		created_at = EXCLUDED.created_at;
END;
$$;


ALTER PROCEDURE public.insert_transformed_performance_data() OWNER TO tomwattley;

--
-- Name: insert_transformed_race_data(); Type: PROCEDURE; Schema: public; Owner: tomwattley
--

CREATE PROCEDURE public.insert_transformed_race_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO public.race_data
    (
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
		race_id,
		course_id,
		created_at
    )
    SELECT
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
		race_id,
		course_id,
		NOW()
    FROM staging.transformed_race_data
    ON CONFLICT (race_id) DO UPDATE SET
		race_time = EXCLUDED.race_time,
		race_date = EXCLUDED.race_date,
		race_title = EXCLUDED.race_title,
		race_type = EXCLUDED.race_type,
		race_class = EXCLUDED.race_class,
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
		total_prize_money = EXCLUDED.total_prize_money,
		first_place_prize_money = EXCLUDED.first_place_prize_money,
		winning_time = EXCLUDED.winning_time,
		time_seconds = EXCLUDED.time_seconds,
		relative_time = EXCLUDED.relative_time,
		relative_to_standard = EXCLUDED.relative_to_standard,
		country = EXCLUDED.country,
		main_race_comment = EXCLUDED.main_race_comment,
		meeting_id = EXCLUDED.meeting_id,
		race_id = EXCLUDED.race_id,
		course_id = EXCLUDED.course_id,
        created_at = EXCLUDED.created_at; 
END;
$$;


ALTER PROCEDURE public.insert_transformed_race_data() OWNER TO tomwattley;

--
-- Name: insert_unioned_non_uk_ire_performance_data_historical(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.insert_unioned_non_uk_ire_performance_data_historical()
    LANGUAGE plpgsql
    AS $$
DECLARE
    last_processed_time timestamp;
BEGIN
    -- Get the last processed time from unioned_performance_data
    SELECT MAX(created_at)
    INTO last_processed_time
    FROM public.unioned_performance_data;
    -- Insert or update data from temporary table into unioned_performance_data
    INSERT INTO public.unioned_performance_data
    SELECT DISTINCT ON (pd.unique_id)
        pd.horse_name, pd.age, pd.horse_sex, pd.draw, pd.headgear, pd.weight_carried, pd.weight_carried_lbs,
        pd.extra_weight, pd.jockey_claim, pd.finishing_position, pd.total_distance_beaten, pd.industry_sp,
        pd.betfair_win_sp, pd.betfair_place_sp, pd.official_rating, pd.ts, pd.rpr, pd.tfr, pd.tfig,
        pd.in_play_high, pd.in_play_low, pd.in_race_comment, pd.tf_comment, pd.tfr_view, rd.race_id,
        pd.horse_id, pd.jockey_id, pd.trainer_id, pd.owner_id, pd.sire_id, pd.dam_id, pd.unique_id,
        rd.race_time, rd.race_date, rd.race_title, rd.race_type, rd.race_class, rd.distance,
        rd.distance_yards, rd.distance_meters, rd.distance_kilometers, rd.conditions, rd.going,
        rd.number_of_runners, rd.hcap_range, rd.age_range, rd.surface, rd.total_prize_money,
        rd.first_place_prize_money, rd.winning_time, rd.time_seconds, rd.relative_time,
        rd.relative_to_standard, rd.country, rd.main_race_comment, rd.meeting_id, rd.course_id,
        c.name as course, d.name as dam, s.name as sire, t.name as trainer, j.name as jockey, hpd.price_change,
        pd.created_at
    FROM non_uk_ire_performance_data pd
    LEFT JOIN non_uk_ire_race_data rd ON pd.race_id = rd.race_id
    LEFT JOIN course c ON pd.course_id = c.id
    LEFT JOIN horse h ON pd.horse_id = h.id
    LEFT JOIN dam d ON pd.dam_id = d.id
    LEFT JOIN sire s ON pd.sire_id = s.id
    LEFT JOIN trainer t ON pd.trainer_id = t.id
    LEFT JOIN jockey j ON pd.jockey_id = j.id
    LEFT JOIN historical_price_data hpd ON pd.unique_id = hpd.unique_id
    WHERE pd.created_at > last_processed_time
    ON CONFLICT (unique_id, race_date) DO UPDATE SET
        horse_name = EXCLUDED.horse_name,
        age = EXCLUDED.age,
        horse_sex = EXCLUDED.horse_sex,
        draw = EXCLUDED.draw,
        headgear = EXCLUDED.headgear,
        weight_carried = EXCLUDED.weight_carried,
        weight_carried_lbs = EXCLUDED.weight_carried_lbs,
        extra_weight = EXCLUDED.extra_weight,
        jockey_claim = EXCLUDED.jockey_claim,
        finishing_position = EXCLUDED.finishing_position,
        total_distance_beaten = EXCLUDED.total_distance_beaten,
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
        tfr_view = EXCLUDED.tfr_view,
        race_id = EXCLUDED.race_id,
        horse_id = EXCLUDED.horse_id,
        jockey_id = EXCLUDED.jockey_id,
        trainer_id = EXCLUDED.trainer_id,
        owner_id = EXCLUDED.owner_id,
        sire_id = EXCLUDED.sire_id,
        dam_id = EXCLUDED.dam_id,
        race_time = EXCLUDED.race_time,
        race_title = EXCLUDED.race_title,
        race_type = EXCLUDED.race_type,
        race_class = EXCLUDED.race_class,
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
        total_prize_money = EXCLUDED.total_prize_money,
        first_place_prize_money = EXCLUDED.first_place_prize_money,
        winning_time = EXCLUDED.winning_time,
        time_seconds = EXCLUDED.time_seconds,
        relative_time = EXCLUDED.relative_time,
        relative_to_standard = EXCLUDED.relative_to_standard,
        country = EXCLUDED.country,
        main_race_comment = EXCLUDED.main_race_comment,
        meeting_id = EXCLUDED.meeting_id,
        course_id = EXCLUDED.course_id,
        course = EXCLUDED.course,
        dam = EXCLUDED.dam,
        sire = EXCLUDED.sire,
        trainer = EXCLUDED.trainer,
        jockey = EXCLUDED.jockey,
        price_move = EXCLUDED.price_move,
        created_at = EXCLUDED.created_at;

END;
$$;


ALTER PROCEDURE public.insert_unioned_non_uk_ire_performance_data_historical() OWNER TO postgres;

--
-- Name: insert_unioned_performance_data_historical(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.insert_unioned_performance_data_historical()
    LANGUAGE plpgsql
    AS $$
DECLARE
    last_processed_time timestamp;
BEGIN
    -- Get the last processed time from unioned_performance_data
    SELECT MAX(created_at)
    INTO last_processed_time
    FROM public.unioned_performance_data;

    -- Create a temporary table for new/updated records
    CREATE TEMPORARY TABLE temp_performance_data AS
    SELECT DISTINCT ON (pd.unique_id)
        pd.horse_name, pd.age, pd.horse_sex, pd.draw, pd.headgear, pd.weight_carried, pd.weight_carried_lbs,
        pd.extra_weight, pd.jockey_claim, pd.finishing_position, pd.total_distance_beaten, pd.industry_sp,
        pd.betfair_win_sp, pd.betfair_place_sp, pd.official_rating, pd.ts, pd.rpr, pd.tfr, pd.tfig,
        pd.in_play_high, pd.in_play_low, pd.in_race_comment, pd.tf_comment, pd.tfr_view, rd.race_id,
        pd.horse_id, pd.jockey_id, pd.trainer_id, pd.owner_id, pd.sire_id, pd.dam_id, pd.unique_id,
        rd.race_time, rd.race_date, rd.race_title, rd.race_type, rd.race_class, rd.distance,
        rd.distance_yards, rd.distance_meters, rd.distance_kilometers, rd.conditions, rd.going,
        rd.number_of_runners, rd.hcap_range, rd.age_range, rd.surface, rd.total_prize_money,
        rd.first_place_prize_money, rd.winning_time, rd.time_seconds, rd.relative_time,
        rd.relative_to_standard, rd.country, rd.main_race_comment, rd.meeting_id, rd.course_id,
        c.name as course, d.name as dam, s.name as sire, t.name as trainer, j.name as jockey, hpd.price_change,
        pd.created_at
    FROM performance_data pd
    LEFT JOIN race_data rd ON pd.race_id = rd.race_id
    LEFT JOIN course c ON pd.course_id = c.id
    LEFT JOIN horse h ON pd.horse_id = h.id
    LEFT JOIN dam d ON pd.dam_id = d.id
    LEFT JOIN sire s ON pd.sire_id = s.id
    LEFT JOIN trainer t ON pd.trainer_id = t.id
    LEFT JOIN jockey j ON pd.jockey_id = j.id
    LEFT JOIN historical_price_data hpd ON pd.unique_id = hpd.unique_id
    WHERE pd.created_at > last_processed_time
    ORDER BY pd.unique_id, rd.race_date DESC;

    -- Amend winning position of first place horse in the temporary table
    UPDATE temp_performance_data up
    SET total_distance_beaten = -sub.distance_beaten
    FROM (
        SELECT race_id, total_distance_beaten AS distance_beaten
        FROM temp_performance_data
        WHERE finishing_position = '2'
        AND total_distance_beaten <> 0
    ) sub
    WHERE up.race_id = sub.race_id
    AND up.finishing_position = '1'
    AND up.total_distance_beaten = 0;

    -- Insert or update data from temporary table into unioned_performance_data
    INSERT INTO public.unioned_performance_data
    SELECT * FROM temp_performance_data
    ON CONFLICT (unique_id, race_date) DO UPDATE SET
        horse_name = EXCLUDED.horse_name,
        age = EXCLUDED.age,
        horse_sex = EXCLUDED.horse_sex,
        draw = EXCLUDED.draw,
        headgear = EXCLUDED.headgear,
        weight_carried = EXCLUDED.weight_carried,
        weight_carried_lbs = EXCLUDED.weight_carried_lbs,
        extra_weight = EXCLUDED.extra_weight,
        jockey_claim = EXCLUDED.jockey_claim,
        finishing_position = EXCLUDED.finishing_position,
        total_distance_beaten = EXCLUDED.total_distance_beaten,
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
        tfr_view = EXCLUDED.tfr_view,
        race_id = EXCLUDED.race_id,
        horse_id = EXCLUDED.horse_id,
        jockey_id = EXCLUDED.jockey_id,
        trainer_id = EXCLUDED.trainer_id,
        owner_id = EXCLUDED.owner_id,
        sire_id = EXCLUDED.sire_id,
        dam_id = EXCLUDED.dam_id,
        race_time = EXCLUDED.race_time,
        race_title = EXCLUDED.race_title,
        race_type = EXCLUDED.race_type,
        race_class = EXCLUDED.race_class,
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
        total_prize_money = EXCLUDED.total_prize_money,
        first_place_prize_money = EXCLUDED.first_place_prize_money,
        winning_time = EXCLUDED.winning_time,
        time_seconds = EXCLUDED.time_seconds,
        relative_time = EXCLUDED.relative_time,
        relative_to_standard = EXCLUDED.relative_to_standard,
        country = EXCLUDED.country,
        main_race_comment = EXCLUDED.main_race_comment,
        meeting_id = EXCLUDED.meeting_id,
        course_id = EXCLUDED.course_id,
        course = EXCLUDED.course,
        dam = EXCLUDED.dam,
        sire = EXCLUDED.sire,
        trainer = EXCLUDED.trainer,
        jockey = EXCLUDED.jockey,
        price_move = EXCLUDED.price_move,
        created_at = EXCLUDED.created_at;

    -- Drop the temporary table
    DROP TABLE temp_performance_data;

END;
$$;


ALTER PROCEDURE public.insert_unioned_performance_data_historical() OWNER TO postgres;

--
-- Name: insert_unioned_performance_data_today(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.insert_unioned_performance_data_today()
    LANGUAGE plpgsql
    AS $$
DECLARE
BEGIN
    -- Insert today's data
    INSERT INTO public.unioned_performance_data
    (
        horse_name, age, horse_sex, draw, headgear, weight_carried, weight_carried_lbs, extra_weight, jockey_claim,
        finishing_position, total_distance_beaten, industry_sp, betfair_win_sp, betfair_place_sp, official_rating,
        ts, rpr, tfr, tfig, in_play_high, in_play_low, in_race_comment, tf_comment, tfr_view, race_id, horse_id,
        jockey_id, trainer_id, owner_id, sire_id, dam_id, unique_id, race_time, race_date, race_title, race_type,
        race_class, distance, distance_yards, distance_meters, distance_kilometers, conditions, going,
        number_of_runners, hcap_range, age_range, surface, total_prize_money, first_place_prize_money, winning_time,
        time_seconds, relative_time, relative_to_standard, country, main_race_comment, meeting_id, course_id,
        course, dam, sire, trainer, jockey, price_move
    )
    SELECT 
        tpd.horse_name, tpd.age, tpd.horse_sex, tpd.draw, tpd.headgear, tpd.weight_carried, tpd.weight_carried_lbs,
        tpd.extra_weight, tpd.jockey_claim, tpd.finishing_position, tpd.total_distance_beaten, tpd.industry_sp,
        tpd.betfair_win_sp, tpd.betfair_place_sp, tpd.official_rating, tpd.ts, tpd.rpr, tpd.tfr, tpd.tfig,
        tpd.in_play_high, tpd.in_play_low, tpd.in_race_comment, tpd.tf_comment, tpd.tfr_view, trd.race_id,
        tpd.horse_id, tpd.jockey_id, tpd.trainer_id, tpd.owner_id, tpd.sire_id, tpd.dam_id, tpd.unique_id,
        trd.race_time, trd.race_date, trd.race_title, trd.race_type, trd.race_class, trd.distance,
        trd.distance_yards, trd.distance_meters, trd.distance_kilometers, trd.conditions, trd.going,
        trd.number_of_runners, trd.hcap_range, trd.age_range, sf.name as surface, trd.total_prize_money,
        trd.first_place_prize_money, trd.winning_time, trd.time_seconds, trd.relative_time,
        trd.relative_to_standard, trd.country, trd.main_race_comment, trd.meeting_id, trd.course_id,
        c.name as course, d.name as dam, s.name as sire, t.name as trainer, j.name as jockey, NULL as price_move
    FROM todays_performance_data tpd
    LEFT JOIN todays_race_data trd ON tpd.race_id = trd.race_id
    LEFT JOIN course c ON tpd.course_id = c.id
    LEFT JOIN surface sf ON c.surface_id = sf.id
    LEFT JOIN horse h ON tpd.horse_id = h.id
    LEFT JOIN dam d ON tpd.dam_id = d.id
    LEFT JOIN sire s ON tpd.sire_id = s.id
    LEFT JOIN trainer t ON tpd.trainer_id = t.id
    LEFT JOIN jockey j ON tpd.jockey_id = j.id;

END;
$$;


ALTER PROCEDURE public.insert_unioned_performance_data_today() OWNER TO postgres;

--
-- Name: select_collateral_form_data_by_race_id(date, integer, date, integer); Type: FUNCTION; Schema: public; Owner: tomwattley
--

CREATE FUNCTION public.select_collateral_form_data_by_race_id(input_date date, input_race_id integer, todays_race_date date, input_horse_id integer) RETURNS TABLE(horse_name character varying, age integer, horse_sex character varying, draw integer, headgear character varying, weight_carried character varying, weight_carried_lbs smallint, extra_weight smallint, jockey_claim smallint, finishing_position character varying, total_distance_beaten numeric, industry_sp character varying, betfair_win_sp numeric, betfair_place_sp numeric, official_rating smallint, ts smallint, rpr smallint, tfr smallint, tfig smallint, in_play_high numeric, in_play_low numeric, in_race_comment text, tf_comment text, tfr_view character varying, race_id integer, horse_id integer, jockey_id integer, trainer_id integer, owner_id integer, sire_id integer, dam_id integer, unique_id character varying, race_time timestamp without time zone, race_date date, race_title character varying, race_type character varying, race_class smallint, distance character varying, distance_yards numeric, distance_meters numeric, distance_kilometers numeric, conditions character varying, going character varying, number_of_runners smallint, hcap_range character varying, age_range character varying, surface character varying, total_prize_money integer, first_place_prize_money integer, winning_time character varying, time_seconds numeric, relative_time numeric, relative_to_standard character varying, country character varying, main_race_comment text, meeting_id character varying, course_id smallint, course character varying, dam character varying, sire character varying, trainer character varying, jockey character varying, data_type character varying, distance_difference numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 
with collateral_form as (
SELECT     
	pd.*,
	'collateral'::character varying as data_type
FROM public.unioned_performance_data pd
WHERE pd.horse_id IN (
    SELECT pd.horse_id
    FROM public.unioned_performance_data pd
    WHERE pd.race_id = input_race_id
)
AND pd.race_date > input_date
AND pd.race_date < todays_race_date
ORDER BY pd.race_date),
race_form as (
	SELECT     
	pd.*,
	'race_form'::character varying as data_type
FROM public.unioned_performance_data pd
WHERE pd.race_id = input_race_id
),
combined as (
SELECT * FROM race_form
UNION
SELECT * FROM collateral_form
ORDER BY horse_name, race_time desc),
distances as (
	SELECT 
	pd.horse_id, 
	pd.total_distance_beaten,
		CASE 
	      WHEN pd.total_distance_beaten <= 0 
	      THEN pd.total_distance_beaten - (SELECT 0) 
	      ELSE pd.total_distance_beaten - 
	  (SELECT pdd.total_distance_beaten 
	   FROM public.unioned_performance_data pdd
	   WHERE pdd.race_id = input_race_id AND pdd.horse_id = input_horse_id) 
	    END AS distance_difference
	FROM public.unioned_performance_data pd
	WHERE pd.race_id = input_race_id
)
SELECT 
	c.horse_name,
	c.age,
	c.horse_sex,
	c.draw,
	c.headgear,
	c.weight_carried,
	c.weight_carried_lbs,
	c.extra_weight,
	c.jockey_claim,
	c.finishing_position,
	c.total_distance_beaten,
	c.industry_sp,
	c.betfair_win_sp,
	c.betfair_place_sp,
	c.official_rating,
	c.ts,  
	c.rpr, 
	c.tfr, 
	c.tfig, 
	c.in_play_high,
	c.in_play_low,
	c.in_race_comment,
	c.tf_comment,
	c.tfr_view,
	c.race_id,
	c.horse_id,
	c.jockey_id,
	c.trainer_id,
	c.owner_id,
	c.sire_id,
	c.dam_id,
	c.unique_id,
	c.race_time,
	c.race_date,
	c.race_title,
	c.race_type,
	c.race_class,
	c.distance,
	c.distance_yards,
	c.distance_meters,
	c.distance_kilometers,
	c.conditions,
	c.going,
	c.number_of_runners,
	c.hcap_range,
	c.age_range,
	c.surface,
	c.total_prize_money,
	c.first_place_prize_money,
	c.winning_time,
	c.time_seconds,
	c.relative_time,
	c.relative_to_standard,
	c.country,
	c.main_race_comment,
	c.meeting_id,
	c.course_id,
	c.course,
	c.dam,
	c.sire,
	c.trainer,
	c.jockey,
	c.data_type,
	d.distance_difference
FROM combined c
LEFT JOIN distances d
ON c.horse_id = d.horse_id
WHERE c.horse_id <> input_horse_id;
END;
$$;


ALTER FUNCTION public.select_collateral_form_data_by_race_id(input_date date, input_race_id integer, todays_race_date date, input_horse_id integer) OWNER TO tomwattley;

--
-- Name: update_horse_betfair_data(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.update_horse_betfair_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Update horses where bf_id and bf_name are null
    UPDATE horse h
    SET 
        bf_id = subquery.horse_id,
        bf_name = subquery.horse_name
    FROM (
        SELECT DISTINCT ON (h.id) h.id, tprd.horse_id, tprd.horse_name
        FROM rp_raw.todays_performance_data tpd
        JOIN horse h ON tpd.horse_id = h.rp_id
        JOIN course c ON tpd.course_id = c.rp_id
        JOIN bf_raw.todays_price_data tprd
            ON tprd.course = c.bf_name
            AND tprd.horse_name = tpd.horse_name
            AND tprd.race_time = tpd.race_timestamp
        WHERE tprd.horse_id IS NOT NULL
          AND h.bf_id IS NULL
          AND h.bf_name IS NULL
    ) AS subquery
    WHERE h.id = subquery.id;
END;
$$;


ALTER PROCEDURE public.update_horse_betfair_data() OWNER TO postgres;

--
-- Name: update_horse_betfair_ids(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.update_horse_betfair_ids()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Update the horse table with Betfair IDs
    UPDATE public.horse h
    SET bf_id = em.bf_horse_id
    FROM bf_raw.entity_matches em
    WHERE h.id = em.horse_id
    AND (h.bf_id IS NULL OR h.bf_id <> em.bf_horse_id);

END;
$$;


ALTER PROCEDURE public.update_horse_betfair_ids() OWNER TO postgres;

--
-- Name: update_todays_price_data(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.update_todays_price_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Insert or update the data in the public table using the view
    INSERT INTO public.todays_performance_data_mat_vw
    	(	
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
			price_change,
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
			data_type,
			created_at
		)
	 SELECT	
			ud.horse_name,
			ud.age,
			ud.horse_sex,
			ud.draw,
			ud.headgear,
			ud.weight_carried,
			ud.weight_carried_lbs,
			ud.extra_weight,
			ud.jockey_claim,
			ud.finishing_position,
			ud.total_distance_beaten,
			ud.industry_sp,
			case 
				when ud.data_type = 'today'
				then bfud.betfair_win_sp
				else ud.betfair_win_sp
			end as betfair_win_sp,
			case 
				when ud.data_type = 'today'
				then bfud.betfair_place_sp
				else ud.betfair_place_sp
			end as betfair_place_sp,
			case 
				when ud.data_type = 'today'
				then bfud.price_change
				else ud.price_change
			end as price_change,
			ud.official_rating,
			ud.ts,
			ud.rpr,
			ud.tfr,
			ud.tfig,
			ud.in_play_high,
			ud.in_play_low,
			ud.in_race_comment,
			ud.tf_comment,
			ud.tfr_view,
			ud.race_id,
			ud.horse_id,
			ud.jockey_id,
			ud.trainer_id,
			ud.owner_id,
			ud.sire_id,
			ud.dam_id,
			ud.unique_id,
			ud.race_time,
			ud.race_date,
			ud.race_title,
			ud.race_type,
			ud.race_class,
			ud.distance,
			ud.distance_yards,
			ud.distance_meters,
			ud.distance_kilometers,
			ud.conditions,
			ud.going,
			ud.number_of_runners,
			ud.hcap_range,
			ud.age_range,
			ud.surface,
			ud.total_prize_money,
			ud.first_place_prize_money,
			ud.winning_time,
			ud.time_seconds,
			ud.relative_time,
			ud.relative_to_standard,
			ud.country,
			ud.main_race_comment,
			ud.meeting_id,
			ud.course_id,
			ud.course,
			ud.dam,
			ud.sire,
			ud.trainer,
			ud.jockey,
			ud.data_type,
			now()
	FROM public.todays_performance_data_mat_vw ud
	LEFT JOIN public.horse h
	ON h.id = ud.horse_id
	LEFT JOIN public.todays_price_data bfud
	ON h.bf_id = bfud.horse_id::character varying;

	DELETE FROM public.todays_performance_data_mat_vw
	WHERE created_at <> (
		SELECT MAX(created_at)
		FROM public.todays_performance_data_mat_vw);
END;
$$;


ALTER PROCEDURE public.update_todays_price_data() OWNER TO postgres;

--
-- Name: get_my_list(); Type: FUNCTION; Schema: rp_raw; Owner: tomwattley
--

CREATE FUNCTION rp_raw.get_my_list() RETURNS text[]
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN ARRAY['value1', 'value2', 'value3'];
END;
$$;


ALTER FUNCTION rp_raw.get_my_list() OWNER TO tomwattley;

--
-- Name: insert_into_joined_non_uk_ire_performance_data(); Type: PROCEDURE; Schema: staging; Owner: tomwattley
--

CREATE PROCEDURE staging.insert_into_joined_non_uk_ire_performance_data()
    LANGUAGE plpgsql
    AS $_$
BEGIN
    INSERT INTO staging.joined_non_uk_ire_performance_data (
		horse_name,
		horse_age,
		jockey_name,
		jockey_claim,
		trainer_name,
		owner_name,
		weight_carried,
		draw,
		finishing_position,
		total_distance_beaten,
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
		race_class,
		conditions,
		distance,
		distance_full,
		going,
		winning_time,
		horse_type,
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
		owner_id,
		race_id,
		unique_id,
		created_at
	)
WITH joined_data AS (
    SELECT 
		nrr.horse_name as horse_name,
		nrr.horse_age as horse_age,
		nrr.jockey_name as jockey_name,
		nrr.jockey_claim as jockey_claim,
		nrr.trainer_name as trainer_name,
		nrr.owner_name as owner_name,
		nrr.horse_weight as weight_carried,
		nrr.draw,
		COALESCE(nrr.finishing_position, tf.finishing_position) AS finishing_position,
		nrr.total_distance_beaten,
		COALESCE(nrr.horse_age, tf.horse_age) AS age,
		COALESCE(nrr.official_rating, tf.official_rating) AS official_rating,
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
		nrr.race_class as race_class,
		nrr.conditions as conditions,
		nrr.distance as distance,
		nrr.distance_full as distance_full,
		nrr.going as going,
		nrr.winning_time as winning_time,
		nrr.horse_type as horse_type,
		nrr.number_of_runners as number_of_runners,
		nrr.total_prize_money as total_prize_money,
		nrr.first_place_prize_money,
		nrr.currency,
		nrr.course_name as course_name,
		nrr.country as country,
	    COALESCE(
	        (regexp_match(nrr.race_title, '\(([^)]+)\)\s*$'))[1],
	        'Unknown'
	    ) AS surface,
		CASE 
		    WHEN tf.tf_rating IS NOT NULL AND tf.tf_rating != '' THEN tf.tf_rating
		    WHEN nrr.rpr_value IS NOT NULL AND nrr.rpr_value != '' THEN nrr.rpr_value
		    ELSE NULL
		END AS tfr,
		
		CASE 
		    WHEN tf.tf_speed_figure IS NOT NULL AND tf.tf_speed_figure != '' THEN tf.tf_speed_figure
		    WHEN nrr.ts_value IS NOT NULL AND nrr.ts_value != '' THEN nrr.ts_value
		    ELSE NULL
		END AS tfig,
		tf.betfair_win_sp as betfair_win_sp,
		tf.betfair_place_sp as betfair_place_sp,
		tf.in_play_prices as in_play_prices,
		COALESCE(tf.tf_comment, nrr.comment) AS tf_comment,
		tf.hcap_range as hcap_range,
		tf.age_range as age_range,
		tf.race_type as race_type,
		tf.main_race_comment as main_race_comment,
		nrr.meeting_id as meeting_id,
		c.id as course_id,
		h.id as horse_id,
		s.id as sire_id,
		d.id as dam_id,
		t.id as trainer_id,
		j.id as jockey_id,
		o.id as owner_id,
		nrr.race_id as race_id,
		nrr.unique_id as unique_id,
        ROW_NUMBER() OVER(PARTITION BY nrr.unique_id ORDER BY nrr.unique_id) AS rn
    FROM rp_raw.non_uk_ire_performance_data nrr
	LEFT JOIN course c ON nrr.course_id = c.rp_id
	LEFT JOIN horse h ON nrr.horse_id = h.rp_id
	LEFT JOIN sire s ON nrr.sire_id = s.rp_id
	LEFT JOIN dam d ON nrr.dam_id = d.rp_id
	LEFT JOIN trainer t ON nrr.trainer_id = t.rp_id
	LEFT JOIN jockey j ON nrr.jockey_id = j.rp_id
	LEFT JOIN owner o ON nrr.owner_id = o.rp_id
    LEFT JOIN tf_raw.non_uk_ire_performance_data tf 
		ON tf.horse_id = h.tf_id
		AND tf.race_date = nrr.race_date
    WHERE h.rp_id IS NOT NULL
)
SELECT 	horse_name,
		horse_age,
		jockey_name,
		jockey_claim,
		trainer_name,
		owner_name,
		weight_carried,
		draw,
		finishing_position,
		total_distance_beaten,
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
		race_class,
		conditions,
		distance,
		distance_full,
		going,
		winning_time,
		horse_type,
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
		owner_id,
		race_id,
		unique_id,
		now()
	FROM joined_data WHERE rn = 1
		ON CONFLICT (unique_id)
		    DO UPDATE SET
				horse_name = EXCLUDED.horse_name,
				horse_age = EXCLUDED.horse_age,
				jockey_name = EXCLUDED.jockey_name,
				jockey_claim = EXCLUDED.jockey_claim,
				trainer_name = EXCLUDED.trainer_name,
				owner_name = EXCLUDED.owner_name,
				weight_carried = EXCLUDED.weight_carried,
				draw = EXCLUDED.draw,
				finishing_position = EXCLUDED.finishing_position,
				total_distance_beaten = EXCLUDED.total_distance_beaten,
				age = EXCLUDED.age,
				official_rating = EXCLUDED.official_rating,
				ts = EXCLUDED.ts,
				rpr = EXCLUDED.rpr,
				industry_sp = EXCLUDED.industry_sp,
				extra_weight = EXCLUDED.extra_weight,
				headgear = EXCLUDED.headgear,
				in_race_comment = EXCLUDED.in_race_comment,
				sire_name = EXCLUDED.sire_name,
				dam_name = EXCLUDED.dam_name,
				dams_sire = EXCLUDED.dams_sire,
				race_date = EXCLUDED.race_date,
				race_title = EXCLUDED.race_title,
				race_time = EXCLUDED.race_time,
				race_timestamp = EXCLUDED.race_timestamp,
				race_class = EXCLUDED.race_class,
				conditions = EXCLUDED.conditions,
				distance = EXCLUDED.distance,
				distance_full = EXCLUDED.distance_full,
				going = EXCLUDED.going,
				winning_time = EXCLUDED.winning_time,
				horse_type = EXCLUDED.horse_type,
				number_of_runners = EXCLUDED.number_of_runners,
				total_prize_money = EXCLUDED.total_prize_money,
				first_place_prize_money = EXCLUDED.first_place_prize_money,
				currency = EXCLUDED.currency,
				country = EXCLUDED.country,
				surface = EXCLUDED.surface,
				tfr = EXCLUDED.tfr,
				tfig = EXCLUDED.tfig,
				betfair_win_sp = EXCLUDED.betfair_win_sp,
				betfair_place_sp = EXCLUDED.betfair_place_sp,
				in_play_prices = EXCLUDED.in_play_prices,
				tf_comment = EXCLUDED.tf_comment,
				hcap_range = EXCLUDED.hcap_range,
				age_range = EXCLUDED.age_range,
				race_type = EXCLUDED.race_type,
				main_race_comment = EXCLUDED.main_race_comment,
				meeting_id = EXCLUDED.meeting_id,
				course_id = EXCLUDED.course_id,
				horse_id = EXCLUDED.horse_id,
				sire_id = EXCLUDED.sire_id,
				dam_id = EXCLUDED.dam_id,
				trainer_id = EXCLUDED.trainer_id,
				jockey_id = EXCLUDED.jockey_id,
				owner_id = EXCLUDED.owner_id,
				race_id = EXCLUDED.race_id;
END;
$_$;


ALTER PROCEDURE staging.insert_into_joined_non_uk_ire_performance_data() OWNER TO tomwattley;

--
-- Name: insert_into_joined_performance_data(); Type: PROCEDURE; Schema: staging; Owner: tomwattley
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
		weight_carried,
		draw,
		finishing_position,
		total_distance_beaten,
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
		race_class,
		conditions,
		distance,
		distance_full,
		going,
		winning_time,
		horse_type,
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
		owner_id,
		race_id,
		unique_id,
		created_at
	)
WITH unique_course AS (
    SELECT DISTINCT ON (rp_id) *
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
unique_owner AS (
    SELECT DISTINCT ON (rp_id) *
    FROM owner
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
		nrr.horse_weight as weight_carried,
		nrr.draw,
		COALESCE(nrr.finishing_position, tf.finishing_position) AS finishing_position,
		nrr.total_distance_beaten,
		COALESCE(nrr.horse_age, tf.horse_age) AS age,
		COALESCE(nrr.official_rating, tf.official_rating) AS official_rating,
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
		nrr.race_class as race_class,
		nrr.conditions as conditions,
		nrr.distance as distance,
		nrr.distance_full as distance_full,
		nrr.going as going,
		nrr.winning_time as winning_time,
		nrr.horse_type as horse_type,
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
		c.id as course_id,
		h.id as horse_id,
		s.id as sire_id,
		d.id as dam_id,
		t.id as trainer_id,
		j.id as jockey_id,
		o.id as owner_id,
		nrr.race_id as race_id,
		nrr.unique_id as unique_id,
        ROW_NUMBER() OVER(PARTITION BY nrr.unique_id ORDER BY nrr.unique_id) AS rn
    FROM new_rp_records nrr
	LEFT JOIN course c ON nrr.course_id = c.rp_id
	LEFT JOIN horse h ON nrr.horse_id = h.rp_id
	LEFT JOIN sire s ON nrr.sire_id = s.rp_id
	LEFT JOIN dam d ON nrr.dam_id = d.rp_id
	LEFT JOIN trainer t ON nrr.trainer_id = t.rp_id
	LEFT JOIN jockey j ON nrr.jockey_id = j.rp_id
	LEFT JOIN owner o ON nrr.owner_id = o.rp_id
    LEFT JOIN tf_raw.performance_data tf ON tf.horse_id = h.tf_id
-- 		AND tf.jockey_id = j.tf_id
-- 		AND tf.trainer_id = t.tf_id
-- 		AND tf.dam_id = d.tf_id
-- 		AND tf.sire_id = s.tf_id
		AND tf.course_id = c.tf_id
		AND tf.race_date = nrr.race_date
    WHERE tf.unique_id IS NOT NULL 
)
SELECT 	horse_name,
		horse_age,
		jockey_name,
		jockey_claim,
		trainer_name,
		owner_name,
		weight_carried,
		draw,
		finishing_position,
		total_distance_beaten,
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
		race_class,
		conditions,
		distance,
		distance_full,
		going,
		winning_time,
		horse_type,
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
		owner_id,
		race_id,
		unique_id,
		now()
	FROM joined_data WHERE rn = 1
		ON CONFLICT (unique_id)
		    DO UPDATE SET
				horse_name = EXCLUDED.horse_name,
				horse_age = EXCLUDED.horse_age,
				jockey_name = EXCLUDED.jockey_name,
				jockey_claim = EXCLUDED.jockey_claim,
				trainer_name = EXCLUDED.trainer_name,
				owner_name = EXCLUDED.owner_name,
				weight_carried = EXCLUDED.weight_carried,
				draw = EXCLUDED.draw,
				finishing_position = EXCLUDED.finishing_position,
				total_distance_beaten = EXCLUDED.total_distance_beaten,
				age = EXCLUDED.age,
				official_rating = EXCLUDED.official_rating,
				ts = EXCLUDED.ts,
				rpr = EXCLUDED.rpr,
				industry_sp = EXCLUDED.industry_sp,
				extra_weight = EXCLUDED.extra_weight,
				headgear = EXCLUDED.headgear,
				in_race_comment = EXCLUDED.in_race_comment,
				sire_name = EXCLUDED.sire_name,
				dam_name = EXCLUDED.dam_name,
				dams_sire = EXCLUDED.dams_sire,
				race_date = EXCLUDED.race_date,
				race_title = EXCLUDED.race_title,
				race_time = EXCLUDED.race_time,
				race_timestamp = EXCLUDED.race_timestamp,
				race_class = EXCLUDED.race_class,
				conditions = EXCLUDED.conditions,
				distance = EXCLUDED.distance,
				distance_full = EXCLUDED.distance_full,
				going = EXCLUDED.going,
				winning_time = EXCLUDED.winning_time,
				horse_type = EXCLUDED.horse_type,
				number_of_runners = EXCLUDED.number_of_runners,
				total_prize_money = EXCLUDED.total_prize_money,
				first_place_prize_money = EXCLUDED.first_place_prize_money,
				currency = EXCLUDED.currency,
				country = EXCLUDED.country,
				surface = EXCLUDED.surface,
				tfr = EXCLUDED.tfr,
				tfig = EXCLUDED.tfig,
				betfair_win_sp = EXCLUDED.betfair_win_sp,
				betfair_place_sp = EXCLUDED.betfair_place_sp,
				in_play_prices = EXCLUDED.in_play_prices,
				tf_comment = EXCLUDED.tf_comment,
				hcap_range = EXCLUDED.hcap_range,
				age_range = EXCLUDED.age_range,
				race_type = EXCLUDED.race_type,
				main_race_comment = EXCLUDED.main_race_comment,
				meeting_id = EXCLUDED.meeting_id,
				course_id = EXCLUDED.course_id,
				horse_id = EXCLUDED.horse_id,
				sire_id = EXCLUDED.sire_id,
				dam_id = EXCLUDED.dam_id,
				trainer_id = EXCLUDED.trainer_id,
				jockey_id = EXCLUDED.jockey_id,
				owner_id = EXCLUDED.owner_id,
				race_id = EXCLUDED.race_id;
END;
$$;


ALTER PROCEDURE staging.insert_into_joined_performance_data() OWNER TO tomwattley;

--
-- Name: insert_into_todays_joined_performance_data(); Type: PROCEDURE; Schema: staging; Owner: tomwattley
--

CREATE PROCEDURE staging.insert_into_todays_joined_performance_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	TRUNCATE staging.todays_joined_performance_data; 
	
    INSERT INTO staging.todays_joined_performance_data (
		horse_name,
		horse_age,
		jockey_name,
		jockey_claim,
		trainer_name,
		owner_name,
		weight_carried,
		draw,
		finishing_position,
		total_distance_beaten,
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
		race_class,
		conditions,
		distance,
		distance_full,
		going,
		winning_time,
		horse_type,
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
		owner_id,
		race_id,
		unique_id,
		created_at
	)
WITH unique_course AS (
    SELECT DISTINCT ON (rp_id) *
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
unique_owner AS (
    SELECT DISTINCT ON (rp_id) *
    FROM owner
),
joined_data AS (
    SELECT 
		trp.horse_name as horse_name,
		trp.horse_age as horse_age,
		trp.jockey_name as jockey_name,
		trp.jockey_claim as jockey_claim,
		trp.trainer_name as trainer_name,
		trp.owner_name as owner_name,
		trp.horse_weight as weight_carried,
		trp.draw,
		COALESCE(trp.finishing_position, ttf.finishing_position) AS finishing_position,
		trp.total_distance_beaten,
		COALESCE(trp.horse_age, ttf.horse_age) AS age,
		COALESCE(trp.official_rating, ttf.official_rating) AS official_rating,
		trp.ts_value as ts,
		trp.rpr_value as rpr,
		trp.horse_price as industry_sp,
		trp.extra_weight as extra_weight,
		trp.headgear,
		trp.comment as in_race_comment,
		trp.sire_name as sire_name,
		trp.dam_name as dam_name,
		trp.dams_sire as dams_sire,
		trp.race_date as race_date,
		trp.race_title as race_title,
		trp.race_time as race_time,
		trp.race_timestamp as race_timestamp,
		trp.race_class as race_class,
		trp.conditions as conditions,
		trp.distance as distance,
		trp.distance_full as distance_full,
		trp.going as going,
		trp.winning_time as winning_time,
		trp.horse_type as horse_type,
		trp.number_of_runners as number_of_runners,
		trp.total_prize_money as total_prize_money,
		trp.first_place_prize_money,
		trp.currency,
		trp.course_name as course_name,
		trp.country as country,
		trp.surface as surface,
		ttf.tf_rating as tfr,
		ttf.tf_speed_figure as tfig,
		ttf.betfair_win_sp as betfair_win_sp,
		ttf.betfair_place_sp as betfair_place_sp,
		ttf.in_play_prices as in_play_prices,
		ttf.tf_comment as tf_comment,
		ttf.hcap_range as hcap_range,
		ttf.age_range as age_range,
		ttf.race_type as race_type,
		ttf.main_race_comment as main_race_comment,
		trp.meeting_id as meeting_id,
		c.id as course_id,
		h.id as horse_id,
		s.id as sire_id,
		d.id as dam_id,
		t.id as trainer_id,
		j.id as jockey_id,
		o.id as owner_id,
		trp.race_id as race_id,
		trp.unique_id as unique_id,
        ROW_NUMBER() OVER(PARTITION BY trp.unique_id ORDER BY trp.unique_id) AS rn
    FROM rp_raw.todays_performance_data trp
	LEFT JOIN course c ON trp.course_id = c.rp_id
	LEFT JOIN horse h ON trp.horse_id = h.rp_id
	LEFT JOIN sire s ON trp.sire_id = s.rp_id
	LEFT JOIN dam d ON trp.dam_id = d.rp_id
	LEFT JOIN trainer t ON trp.trainer_id = t.rp_id
	LEFT JOIN jockey j ON trp.jockey_id = j.rp_id
	LEFT JOIN owner o ON trp.owner_id = o.rp_id
    LEFT JOIN tf_raw.todays_performance_data ttf ON ttf.horse_id = h.tf_id
		AND ttf.course_id = c.tf_id
		AND ttf.race_timestamp = trp.race_timestamp
    WHERE ttf.unique_id IS NOT NULL 
)
SELECT 	horse_name,
		horse_age,
		jockey_name,
		jockey_claim,
		trainer_name,
		owner_name,
		weight_carried,
		draw,
		finishing_position,
		total_distance_beaten,
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
		race_class,
		conditions,
		distance,
		distance_full,
		going,
		winning_time,
		horse_type,
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
		owner_id,
		race_id,
		unique_id,
		now()
	FROM joined_data WHERE rn = 1;
END;
$$;


ALTER PROCEDURE staging.insert_into_todays_joined_performance_data() OWNER TO tomwattley;

--
-- Name: update_todays_price_data(); Type: PROCEDURE; Schema: staging; Owner: postgres
--

CREATE PROCEDURE staging.update_todays_price_data()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Insert or update the data in the public table using the view
    INSERT INTO public.todays_price_data
    	(	
			horse_id, 
			race_time, 
			race_date, 
			horse_name, 
			market_id_win, 
			market_id_place, 
			runners_unique_id, 
			betfair_win_sp, 
			betfair_place_sp, 
			price_change
		)
	with latest_price_data_vw as (
	 SELECT s.horse_id,
	    s.race_time,
	    s.race_date,
	    s.horse_name,
	    s.market_id_win,
	    s.market_id_place,
	    s.runners_unique_id,
	    s.betfair_win_sp,
	    s.betfair_place_sp,
	        CASE
	            WHEN s.price_change IS NOT NULL AND p.price_change IS NOT NULL THEN (s.price_change + p.price_change) / 2::numeric
	            ELSE COALESCE(s.price_change, p.price_change, 0::numeric)
	        END AS price_change
	   FROM todays_price_data p
	     FULL JOIN staging.todays_transformed_price_data s ON p.horse_id = s.horse_id)

    SELECT 
			horse_id, 
			race_time, 
			race_date, 
			horse_name, 
			market_id_win, 
			market_id_place, 
			runners_unique_id, 
			betfair_win_sp, 
			betfair_place_sp, 
			price_change
    FROM latest_price_data_vw
    ON CONFLICT (horse_id)
    DO UPDATE SET
        betfair_win_sp = EXCLUDED.betfair_win_sp,
        betfair_place_sp = EXCLUDED.betfair_place_sp,
        price_change = EXCLUDED.price_change;
END;
$$;


ALTER PROCEDURE staging.update_todays_price_data() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: course; Type: TABLE; Schema: backup; Owner: postgres
--

CREATE TABLE backup.course (
    name character varying(132),
    id smallint,
    rp_name character varying(132),
    rp_id character varying(4),
    tf_name character varying(132),
    tf_id character varying(4),
    country_id character varying(4),
    error_id smallint,
    surface_id smallint,
    bf_name character varying(132),
    bf_id integer
);


ALTER TABLE backup.course OWNER TO postgres;

--
-- Name: public_race_data; Type: TABLE; Schema: backup; Owner: postgres
--

CREATE TABLE backup.public_race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id integer,
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE backup.public_race_data OWNER TO postgres;

--
-- Name: rp_raw_performance_data; Type: TABLE; Schema: backup; Owner: tomwattley
--

CREATE TABLE backup.rp_raw_performance_data (
    race_timestamp timestamp without time zone,
    race_date character varying(32),
    course_name character varying(132),
    race_class character varying(132),
    horse_name character varying(132),
    horse_type character varying(16),
    horse_age character varying(16),
    headgear character varying(16),
    conditions character varying(32),
    horse_price character varying(16),
    race_title text,
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    official_rating character varying(16),
    horse_weight character varying(16),
    draw character varying(16),
    country character varying(16),
    surface character varying(16),
    finishing_position character varying(16),
    total_distance_beaten character varying(32),
    ts_value character varying(16),
    rpr_value character varying(16),
    extra_weight numeric(10,2),
    comment text,
    race_time character varying(32),
    currency character varying(16),
    course character varying(132),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    owner_name character varying(132),
    horse_id character varying(32),
    trainer_id character varying(32),
    jockey_id character varying(32),
    sire_id character varying(32),
    dam_id character varying(32),
    dams_sire_id character varying(32),
    owner_id character varying(32),
    race_id character varying(32),
    course_id character varying(32),
    meeting_id character varying(132),
    unique_id character varying(132),
    debug_link text,
    created_at timestamp without time zone
);


ALTER TABLE backup.rp_raw_performance_data OWNER TO tomwattley;

--
-- Name: rp_raw_performance_data_deduped; Type: TABLE; Schema: backup; Owner: tomwattley
--

CREATE TABLE backup.rp_raw_performance_data_deduped (
    race_timestamp timestamp without time zone,
    race_date character varying(32),
    course_name character varying(132),
    race_class character varying(132),
    horse_name character varying(132),
    horse_type character varying(16),
    horse_age character varying(16),
    headgear character varying(16),
    conditions character varying(32),
    horse_price character varying(16),
    race_title text,
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    official_rating character varying(16),
    horse_weight character varying(16),
    draw character varying(16),
    country character varying(16),
    surface character varying(16),
    finishing_position character varying(16),
    total_distance_beaten character varying(32),
    ts_value character varying(16),
    rpr_value character varying(16),
    extra_weight numeric(10,2),
    comment text,
    race_time character varying(32),
    currency character varying(16),
    course character varying(132),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    owner_name character varying(132),
    horse_id character varying(32),
    trainer_id character varying(32),
    jockey_id character varying(32),
    sire_id character varying(32),
    dam_id character varying(32),
    dams_sire_id character varying(32),
    owner_id character varying(32),
    race_id character varying(32),
    course_id character varying(32),
    meeting_id character varying(132),
    unique_id character varying(132),
    debug_link text,
    created_at timestamp without time zone
);


ALTER TABLE backup.rp_raw_performance_data_deduped OWNER TO tomwattley;

--
-- Name: tf_raw_performance_data; Type: TABLE; Schema: backup; Owner: tomwattley
--

CREATE TABLE backup.tf_raw_performance_data (
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
    unique_id character varying(132)
);


ALTER TABLE backup.tf_raw_performance_data OWNER TO tomwattley;

--
-- Name: tf_raw_performance_data_deduped; Type: TABLE; Schema: backup; Owner: tomwattley
--

CREATE TABLE backup.tf_raw_performance_data_deduped (
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
    unique_id character varying(132)
);


ALTER TABLE backup.tf_raw_performance_data_deduped OWNER TO tomwattley;

--
-- Name: selections; Type: TABLE; Schema: betting; Owner: tomwattley
--

CREATE TABLE betting.selections (
    race_date date,
    race_id integer,
    horse_id integer,
    betting_type character varying(132),
    created_at timestamp without time zone,
    session_id integer
);


ALTER TABLE betting.selections OWNER TO tomwattley;

--
-- Name: selections_info; Type: TABLE; Schema: betting; Owner: postgres
--

CREATE TABLE betting.selections_info (
    betting_type character varying(132),
    session_id integer,
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp with time zone
);


ALTER TABLE betting.selections_info OWNER TO postgres;

--
-- Name: todays_price_data; Type: TABLE; Schema: bf_raw; Owner: tomwattley
--

CREATE TABLE bf_raw.todays_price_data (
    race_time timestamp without time zone,
    race_date date,
    horse_id integer,
    horse_name character varying(132),
    course character varying(132),
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    created_at timestamp without time zone,
    status character varying(64),
    market_id_win character varying(32),
    market_id_place character varying(32)
);


ALTER TABLE bf_raw.todays_price_data OWNER TO tomwattley;

--
-- Name: active_price_data; Type: VIEW; Schema: bf_raw; Owner: postgres
--

CREATE VIEW bf_raw.active_price_data AS
 SELECT race_time,
    race_date,
    horse_id,
    horse_name,
    course,
    betfair_win_sp,
    betfair_place_sp,
    created_at,
    status,
    market_id_win,
    market_id_place
   FROM bf_raw.todays_price_data
  WHERE ((race_date = (CURRENT_DATE AT TIME ZONE 'Europe/London'::text)) AND ((status)::text = 'ACTIVE'::text) AND (created_at >= ((CURRENT_DATE AT TIME ZONE 'Europe/London'::text) + ('10:00:00'::time without time zone)::interval)) AND (race_time > (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/London'::text)))
  ORDER BY created_at;


ALTER VIEW bf_raw.active_price_data OWNER TO postgres;

--
-- Name: historical_price_data; Type: TABLE; Schema: bf_raw; Owner: postgres
--

CREATE TABLE bf_raw.historical_price_data (
    horse text,
    course text,
    race_time timestamp with time zone,
    race_type text,
    runner_id bigint,
    race_key text,
    bf_unique_id text,
    min_price double precision,
    max_price double precision,
    latest_price double precision,
    earliest_price double precision,
    price_change double precision,
    non_runners boolean,
    race_date date
);


ALTER TABLE bf_raw.historical_price_data OWNER TO postgres;

--
-- Name: matched_historical_price_data; Type: TABLE; Schema: bf_raw; Owner: postgres
--

CREATE TABLE bf_raw.matched_historical_price_data (
    race_time timestamp with time zone,
    race_date text,
    horse_name text,
    horse_id text,
    course_id text,
    meeting_id text,
    unique_id text,
    race_id text,
    runner_id bigint,
    race_key text,
    bf_unique_id text,
    price_change double precision
);


ALTER TABLE bf_raw.matched_historical_price_data OWNER TO postgres;

--
-- Name: course; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.course (
    name character varying(132),
    id smallint NOT NULL,
    rp_name character varying(132),
    rp_id character varying(4),
    tf_name character varying(132),
    tf_id character varying(4),
    country_id character varying(4),
    error_id smallint,
    surface_id smallint,
    bf_name character varying(132),
    bf_id integer
);


ALTER TABLE public.course OWNER TO tomwattley;

--
-- Name: horse; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.horse (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL,
    bf_id character varying(32)
);


ALTER TABLE public.horse OWNER TO tomwattley;

--
-- Name: missing_horses; Type: VIEW; Schema: bf_raw; Owner: postgres
--

CREATE VIEW bf_raw.missing_horses AS
 SELECT bf.race_time,
    bf.horse_id,
    bf.horse_name,
    c.id AS course_id
   FROM ((bf_raw.todays_price_data bf
     LEFT JOIN public.course c ON (((bf.course)::text = (c.bf_name)::text)))
     LEFT JOIN public.horse h ON ((bf.horse_id = (h.bf_id)::integer)))
  WHERE (((bf.status)::text = 'ACTIVE'::text) AND (h.bf_id IS NULL) AND (bf.race_date = CURRENT_DATE));


ALTER VIEW bf_raw.missing_horses OWNER TO postgres;

--
-- Name: historical_price_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.historical_price_data (
    race_time timestamp without time zone,
    race_date character varying(32),
    horse_name character varying(132),
    horse_id character varying(32),
    course_id character varying(32),
    meeting_id character varying(132),
    race_id character varying(32),
    runner_id integer,
    race_key character varying(132),
    unique_id character varying(132) NOT NULL,
    bf_unique_id character varying(132) NOT NULL,
    price_change numeric(4,2)
);


ALTER TABLE public.historical_price_data OWNER TO postgres;

--
-- Name: missing_price_data; Type: VIEW; Schema: bf_raw; Owner: postgres
--

CREATE VIEW bf_raw.missing_price_data AS
 SELECT hpd.horse,
    hpd.course,
    hpd.race_time,
    hpd.race_type,
    hpd.runner_id,
    hpd.race_key,
    hpd.bf_unique_id,
    hpd.price_change,
    hpd.race_date
   FROM (bf_raw.historical_price_data hpd
     LEFT JOIN public.historical_price_data phd ON ((hpd.bf_unique_id = (phd.bf_unique_id)::text)))
  WHERE (phd.bf_unique_id IS NULL);


ALTER VIEW bf_raw.missing_price_data OWNER TO postgres;

--
-- Name: missing_price_data_tmp; Type: TABLE; Schema: bf_raw; Owner: postgres
--

CREATE TABLE bf_raw.missing_price_data_tmp (
    horse text,
    course text,
    race_time timestamp with time zone,
    race_type text,
    runner_id bigint,
    race_key text,
    bf_unique_id text,
    price_change double precision,
    race_date date
);


ALTER TABLE bf_raw.missing_price_data_tmp OWNER TO postgres;

--
-- Name: performance_data; Type: TABLE; Schema: rp_raw; Owner: tomwattley
--

CREATE TABLE rp_raw.performance_data (
    race_timestamp timestamp without time zone,
    race_date character varying(32),
    course_name character varying(132),
    race_class character varying(132),
    horse_name character varying(132),
    horse_type character varying(16),
    horse_age character varying(16),
    headgear character varying(16),
    conditions character varying(32),
    horse_price character varying(16),
    race_title text,
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    official_rating character varying(16),
    horse_weight character varying(16),
    draw character varying(16),
    country character varying(16),
    surface character varying(16),
    finishing_position character varying(16),
    total_distance_beaten character varying(32),
    ts_value character varying(16),
    rpr_value character varying(16),
    extra_weight numeric(10,2),
    comment text,
    race_time character varying(32),
    currency character varying(16),
    course character varying(132),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    owner_name character varying(132),
    horse_id character varying(32),
    trainer_id character varying(32),
    jockey_id character varying(32),
    sire_id character varying(32),
    dam_id character varying(32),
    dams_sire_id character varying(32),
    owner_id character varying(32),
    race_id character varying(32),
    course_id character varying(32),
    meeting_id character varying(132),
    unique_id character varying(132),
    debug_link text,
    created_at timestamp without time zone
);


ALTER TABLE rp_raw.performance_data OWNER TO tomwattley;

--
-- Name: missing_price_performance_data; Type: VIEW; Schema: bf_raw; Owner: postgres
--

CREATE VIEW bf_raw.missing_price_performance_data AS
 SELECT pd.race_timestamp,
    pd.race_date,
    pd.course_name,
    pd.race_class,
    pd.horse_name,
    pd.horse_id,
    pd.course_id,
    pd.race_id,
    pd.meeting_id,
    pd.unique_id,
    c.bf_name,
    c.bf_id
   FROM (rp_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_id)::text)))
  WHERE ((pd.race_date)::date IN ( SELECT DISTINCT missing_price_data.race_date
           FROM bf_raw.missing_price_data));


ALTER VIEW bf_raw.missing_price_performance_data OWNER TO postgres;

--
-- Name: performance_data; Type: TABLE; Schema: tf_raw; Owner: tomwattley
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
    unique_id character varying(132)
);


ALTER TABLE tf_raw.performance_data OWNER TO tomwattley;

--
-- Name: missing_price_performance_data_tmp; Type: VIEW; Schema: bf_raw; Owner: postgres
--

CREATE VIEW bf_raw.missing_price_performance_data_tmp AS
 SELECT DISTINCT ON (pd.unique_id) pd.race_timestamp,
    pd.race_date,
    pd.course_name,
    pd.race_class,
    pd.horse_name AS rp_horse_name,
    pd.horse_id,
    pd.course_id,
    pd.race_id,
    pd.meeting_id,
    pd.unique_id,
    c.bf_name,
    c.bf_id,
    tf.horse_name AS tf_horse_name
   FROM (((rp_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_id)::text)))
     LEFT JOIN public.horse h ON (((pd.horse_id)::text = (h.rp_id)::text)))
     LEFT JOIN tf_raw.performance_data tf ON (((h.tf_id)::text = (tf.horse_id)::text)))
  WHERE ((pd.race_date)::date IN ( SELECT DISTINCT missing_price_data.race_date
           FROM bf_raw.missing_price_data));


ALTER VIEW bf_raw.missing_price_performance_data_tmp OWNER TO postgres;

--
-- Name: staging_entity_unmatched; Type: TABLE; Schema: errors; Owner: tomwattley
--

CREATE TABLE errors.staging_entity_unmatched (
    entity text,
    race_timestamp timestamp without time zone,
    name text,
    debug_link text
);


ALTER TABLE errors.staging_entity_unmatched OWNER TO tomwattley;

--
-- Name: staging_todays_transformed_performance_data_rejected; Type: TABLE; Schema: errors; Owner: tomwattley
--

CREATE TABLE errors.staging_todays_transformed_performance_data_rejected (
    race_time timestamp without time zone,
    race_date date,
    horse_name text,
    age bigint,
    horse_sex text,
    draw bigint,
    headgear text,
    weight_carried text,
    weight_carried_lbs bigint,
    extra_weight bigint,
    jockey_claim text,
    finishing_position text,
    total_distance_beaten double precision,
    industry_sp text,
    betfair_win_sp double precision,
    betfair_place_sp double precision,
    official_rating bigint,
    ts bigint,
    rpr bigint,
    tfig bigint,
    tfr bigint,
    in_play_high double precision,
    in_play_low double precision,
    in_race_comment text,
    tf_comment text,
    tfr_view text,
    course_id bigint,
    horse_id bigint,
    jockey_id bigint,
    trainer_id bigint,
    owner_id bigint,
    sire_id bigint,
    dam_id bigint,
    unique_id text,
    created_at timestamp without time zone
);


ALTER TABLE errors.staging_todays_transformed_performance_data_rejected OWNER TO tomwattley;

--
-- Name: staging_transformed_performance_data_rejected; Type: TABLE; Schema: errors; Owner: tomwattley
--

CREATE TABLE errors.staging_transformed_performance_data_rejected (
    race_time timestamp without time zone,
    race_date date,
    horse_name character varying(132),
    age integer,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
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
    tfr_view character varying(16),
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


ALTER TABLE errors.staging_transformed_performance_data_rejected OWNER TO tomwattley;

--
-- Name: todays_performance_data; Type: TABLE; Schema: errors; Owner: tomwattley
--

CREATE TABLE errors.todays_performance_data (
    error_url text,
    error_message text,
    error_processing_time timestamp without time zone
);


ALTER TABLE errors.todays_performance_data OWNER TO tomwattley;

--
-- Name: performance_data; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.performance_data (
    race_time timestamp without time zone NOT NULL,
    race_date date NOT NULL,
    horse_name character varying(132) NOT NULL,
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer NOT NULL,
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


ALTER TABLE public.performance_data OWNER TO tomwattley;

--
-- Name: missing_raw_data; Type: VIEW; Schema: metrics; Owner: tomwattley
--

CREATE VIEW metrics.missing_raw_data AS
 SELECT rpd.race_timestamp,
    rpd.horse_name
   FROM (rp_raw.performance_data rpd
     LEFT JOIN public.performance_data pd ON (((rpd.unique_id)::text = (pd.unique_id)::text)))
  WHERE (pd.unique_id IS NULL);


ALTER VIEW metrics.missing_raw_data OWNER TO tomwattley;

--
-- Name: joined_performance_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.joined_performance_data (
    horse_name character varying(132),
    horse_age character varying(16),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    owner_name character varying(132),
    weight_carried character varying(16),
    draw character varying(16),
    finishing_position character varying(16),
    total_distance_beaten character varying(16),
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
    race_class character varying(32),
    conditions character varying(32),
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    winning_time character varying(32),
    horse_type character varying(32),
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
    course_id smallint,
    horse_id integer,
    sire_id integer,
    dam_id integer,
    trainer_id integer,
    jockey_id integer,
    owner_id integer,
    race_id character varying(32),
    unique_id character varying(132),
    created_at timestamp without time zone
);


ALTER TABLE staging.joined_performance_data OWNER TO tomwattley;

--
-- Name: missing_raw_to_stg; Type: VIEW; Schema: metrics; Owner: tomwattley
--

CREATE VIEW metrics.missing_raw_to_stg AS
 SELECT rpd.race_timestamp,
    rpd.horse_name
   FROM (rp_raw.performance_data rpd
     LEFT JOIN staging.joined_performance_data spd ON (((rpd.unique_id)::text = (spd.unique_id)::text)))
  WHERE (spd.unique_id IS NULL);


ALTER VIEW metrics.missing_raw_to_stg OWNER TO tomwattley;

--
-- Name: transformed_performance_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.transformed_performance_data (
    race_time timestamp without time zone,
    race_date date,
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
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


ALTER TABLE staging.transformed_performance_data OWNER TO tomwattley;

--
-- Name: missing_stg_to_cns; Type: VIEW; Schema: metrics; Owner: tomwattley
--

CREATE VIEW metrics.missing_stg_to_cns AS
 SELECT spd.race_time,
    spd.horse_name
   FROM (staging.transformed_performance_data spd
     LEFT JOIN public.performance_data pd ON (((spd.unique_id)::text = (pd.unique_id)::text)))
  WHERE (pd.unique_id IS NULL);


ALTER VIEW metrics.missing_stg_to_cns OWNER TO tomwattley;

--
-- Name: todays_performance_data; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.todays_performance_data (
    race_time timestamp without time zone NOT NULL,
    race_date date NOT NULL,
    horse_name character varying(132) NOT NULL,
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer NOT NULL,
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


ALTER TABLE public.todays_performance_data OWNER TO tomwattley;

--
-- Name: todays_performance_data; Type: TABLE; Schema: rp_raw; Owner: tomwattley
--

CREATE TABLE rp_raw.todays_performance_data (
    race_timestamp timestamp without time zone,
    race_date character varying(32),
    course_name character varying(132),
    race_class character varying(132),
    horse_name character varying(132),
    horse_type character varying(16),
    horse_age character varying(16),
    headgear character varying(16),
    conditions character varying(32),
    horse_price character varying(16),
    race_title text,
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    official_rating character varying(16),
    horse_weight character varying(16),
    draw character varying(16),
    country character varying(16),
    surface character varying(16),
    finishing_position character varying(16),
    total_distance_beaten character varying(32),
    ts_value character varying(16),
    rpr_value character varying(16),
    extra_weight numeric(10,2),
    comment text,
    race_time character varying(32),
    currency character varying(16),
    course character varying(132),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    owner_name character varying(132),
    horse_id character varying(32),
    trainer_id character varying(32),
    jockey_id character varying(32),
    sire_id character varying(32),
    dam_id character varying(32),
    dams_sire_id character varying(32),
    owner_id character varying(32),
    race_id character varying(32),
    course_id character varying(32),
    meeting_id character varying(132),
    unique_id character varying(132),
    debug_link text,
    created_at timestamp without time zone
);


ALTER TABLE rp_raw.todays_performance_data OWNER TO tomwattley;

--
-- Name: todays_performance_data; Type: TABLE; Schema: tf_raw; Owner: tomwattley
--

CREATE TABLE tf_raw.todays_performance_data (
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
    unique_id character varying(132)
);


ALTER TABLE tf_raw.todays_performance_data OWNER TO tomwattley;

--
-- Name: missing_todays_races; Type: VIEW; Schema: metrics; Owner: postgres
--

CREATE VIEW metrics.missing_todays_races AS
 WITH unique_rp_times AS (
         SELECT DISTINCT ON (todays_performance_data.race_timestamp, todays_performance_data.course_id) todays_performance_data.race_timestamp,
            todays_performance_data.course_id
           FROM rp_raw.todays_performance_data
          WHERE ((date(todays_performance_data.race_timestamp) = CURRENT_DATE) AND ((todays_performance_data.country)::text = ANY ((ARRAY['UK'::character varying, 'IRE'::character varying])::text[])))
        ), unique_tf_times AS (
         SELECT DISTINCT ON (todays_performance_data.race_timestamp, todays_performance_data.course_id) todays_performance_data.race_timestamp,
            todays_performance_data.course_id
           FROM tf_raw.todays_performance_data
          WHERE (date(todays_performance_data.race_timestamp) = CURRENT_DATE)
        ), unique_public_times AS (
         SELECT DISTINCT ON (todays_performance_data.race_time, todays_performance_data.course_id) todays_performance_data.race_time,
            todays_performance_data.course_id
           FROM public.todays_performance_data
          WHERE (date(todays_performance_data.race_time) = CURRENT_DATE)
        )
 SELECT rp.race_timestamp
   FROM (((unique_rp_times rp
     LEFT JOIN public.course c ON (((c.rp_id)::text = (rp.course_id)::text)))
     LEFT JOIN unique_tf_times tf ON ((((c.tf_id)::text = (tf.course_id)::text) AND (tf.race_timestamp = rp.race_timestamp))))
     LEFT JOIN unique_public_times pt ON (((pt.course_id = c.id) AND (pt.race_time = rp.race_timestamp))))
  WHERE ((tf.course_id IS NULL) OR (pt.course_id IS NULL));


ALTER VIEW metrics.missing_todays_races OWNER TO postgres;

--
-- Name: processing_times; Type: TABLE; Schema: metrics; Owner: tomwattley
--

CREATE TABLE metrics.processing_times (
    job_name character varying(132),
    processed_at timestamp without time zone
);


ALTER TABLE metrics.processing_times OWNER TO tomwattley;

--
-- Name: record_count_differences_vw; Type: VIEW; Schema: metrics; Owner: tomwattley
--

CREATE VIEW metrics.record_count_differences_vw AS
 WITH rp_course_counts AS (
         SELECT c.name AS course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.error_id AS course_id
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_id)::text)))
          GROUP BY c.name, pd.race_date, c.error_id
        ), tf_course_counts AS (
         SELECT c.name AS course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.error_id AS course_id
           FROM (tf_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_id)::text)))
          GROUP BY c.name, pd.race_date, c.error_id
        )
 SELECT COALESCE(rp.course_name, tf.course_name) AS course,
    COALESCE(rp.race_date, tf.race_date) AS race_date,
    rp.num_records AS rp_num_records,
    tf.num_records AS tf_num_records
   FROM (rp_course_counts rp
     JOIN tf_course_counts tf ON ((((rp.race_date)::text = (tf.race_date)::text) AND ((rp.course_id)::text = (tf.course_id)::text))))
  WHERE ((rp.num_records <> tf.num_records) OR (rp.num_records IS NULL) OR ((tf.num_records IS NULL) AND ((rp.race_date)::date > '2010-01-01'::date)))
  ORDER BY COALESCE(rp.race_date, tf.race_date) DESC, COALESCE(rp.course_name, tf.course_name), rp.course_id;


ALTER VIEW metrics.record_count_differences_vw OWNER TO tomwattley;

--
-- Name: country; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.country (
    country_id integer,
    country_name character varying(132)
);


ALTER TABLE public.country OWNER TO tomwattley;

--
-- Name: course_backup; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.course_backup (
    name character varying(132),
    id smallint,
    rp_name character varying(132),
    rp_id character varying(4),
    tf_name character varying(132),
    tf_id character varying(4),
    country_id character varying(4),
    error_id smallint,
    surface_id smallint,
    bf_name character varying(132),
    bf_id integer
);


ALTER TABLE public.course_backup OWNER TO postgres;

--
-- Name: dam; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.dam (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.dam OWNER TO tomwattley;

--
-- Name: dam_dam_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.dam_dam_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dam_dam_id_seq OWNER TO tomwattley;

--
-- Name: dam_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.dam_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dam_id_seq OWNER TO tomwattley;

--
-- Name: dam_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomwattley
--

ALTER SEQUENCE public.dam_id_seq OWNED BY public.dam.id;


--
-- Name: distance; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.distance (
    distance text,
    distance_yards bigint,
    distance_meters double precision,
    distance_kilometers double precision
);


ALTER TABLE public.distance OWNER TO postgres;

--
-- Name: feedback_date; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.feedback_date (
    today_date date
);


ALTER TABLE public.feedback_date OWNER TO postgres;

--
-- Name: feedback_performance_data_mat_vw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.feedback_performance_data_mat_vw (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    industry_sp character varying(16),
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    price_change numeric(4,2),
    official_rating smallint,
    ts smallint,
    rpr smallint,
    tfr smallint,
    tfig smallint,
    in_play_high numeric(6,2),
    in_play_low numeric(6,2),
    in_race_comment text,
    tf_comment text,
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    data_type character varying
);


ALTER TABLE public.feedback_performance_data_mat_vw OWNER TO postgres;

--
-- Name: historical_unioned_performance_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.historical_unioned_performance_data (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_change numeric(4,2)
);


ALTER TABLE public.historical_unioned_performance_data OWNER TO postgres;

--
-- Name: horse_horse_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.horse_horse_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.horse_horse_id_seq OWNER TO tomwattley;

--
-- Name: horse_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.horse_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.horse_id_seq OWNER TO tomwattley;

--
-- Name: horse_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomwattley
--

ALTER SEQUENCE public.horse_id_seq OWNED BY public.horse.id;


--
-- Name: jockey; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.jockey (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.jockey OWNER TO tomwattley;

--
-- Name: jockey_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.jockey_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.jockey_id_seq OWNER TO tomwattley;

--
-- Name: jockey_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomwattley
--

ALTER SEQUENCE public.jockey_id_seq OWNED BY public.jockey.id;


--
-- Name: jockey_jockey_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.jockey_jockey_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.jockey_jockey_id_seq OWNER TO tomwattley;

--
-- Name: non_uk_ire_performance_data; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.non_uk_ire_performance_data (
    race_time timestamp without time zone NOT NULL,
    race_date date NOT NULL,
    horse_name character varying(132) NOT NULL,
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(64),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    industry_sp character varying(64),
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
    tfr_view character varying(64),
    race_id integer NOT NULL,
    course_id smallint NOT NULL,
    horse_id integer NOT NULL,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132) NOT NULL,
    created_at timestamp without time zone NOT NULL
);


ALTER TABLE public.non_uk_ire_performance_data OWNER TO tomwattley;

--
-- Name: joined_non_uk_ire_performance_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.joined_non_uk_ire_performance_data (
    horse_name character varying(132),
    horse_age character varying(64),
    jockey_name character varying(132),
    jockey_claim character varying(64),
    trainer_name character varying(132),
    owner_name character varying(132),
    weight_carried character varying(64),
    draw character varying(64),
    finishing_position character varying(64),
    total_distance_beaten character varying(64),
    age character varying(64),
    official_rating character varying(64),
    ts character varying(64),
    rpr character varying(64),
    industry_sp character varying(64),
    extra_weight numeric,
    headgear character varying(64),
    in_race_comment text,
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    race_date character varying(64),
    race_title text,
    race_time character varying(64),
    race_timestamp timestamp without time zone,
    race_class character varying(64),
    conditions character varying(64),
    distance character varying(64),
    distance_full character varying(64),
    going character varying(64),
    winning_time character varying(64),
    horse_type character varying(64),
    number_of_runners character varying(64),
    total_prize_money integer,
    first_place_prize_money integer,
    currency character varying(64),
    country character varying(64),
    surface character varying(64),
    tfr character varying(64),
    tfig character varying(64),
    betfair_win_sp character varying(64),
    betfair_place_sp character varying(64),
    in_play_prices character varying(64),
    tf_comment text,
    hcap_range character varying(64),
    age_range character varying(64),
    race_type character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    horse_id integer,
    sire_id integer,
    dam_id integer,
    trainer_id integer,
    jockey_id integer,
    owner_id integer,
    race_id character varying(64),
    unique_id character varying(132),
    created_at timestamp without time zone
);


ALTER TABLE staging.joined_non_uk_ire_performance_data OWNER TO tomwattley;

--
-- Name: missing_non_uk_ire_performance_data_vw; Type: VIEW; Schema: public; Owner: tomwattley
--

CREATE VIEW public.missing_non_uk_ire_performance_data_vw AS
 SELECT spd.horse_name,
    spd.horse_age,
    spd.jockey_name,
    spd.jockey_claim,
    spd.trainer_name,
    spd.owner_name,
    spd.weight_carried,
    spd.draw,
    spd.finishing_position,
    spd.total_distance_beaten,
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
    spd.race_class,
    spd.conditions,
    spd.distance,
    spd.distance_full,
    spd.going,
    spd.winning_time,
    spd.horse_type,
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
    spd.owner_id,
    spd.race_id,
    spd.unique_id,
    spd.created_at,
    'historcal'::text AS data_type
   FROM (staging.joined_non_uk_ire_performance_data spd
     LEFT JOIN public.non_uk_ire_performance_data pd ON (((spd.unique_id)::text = (pd.unique_id)::text)))
  WHERE ((pd.unique_id IS NULL) AND ((spd.race_date)::text > '2010-01-01'::text));


ALTER VIEW public.missing_non_uk_ire_performance_data_vw OWNER TO tomwattley;

--
-- Name: missing_performance_data_vw; Type: VIEW; Schema: public; Owner: tomwattley
--

CREATE VIEW public.missing_performance_data_vw AS
 SELECT spd.horse_name,
    spd.horse_age,
    spd.jockey_name,
    spd.jockey_claim,
    spd.trainer_name,
    spd.owner_name,
    spd.weight_carried,
    spd.draw,
    spd.finishing_position,
    spd.total_distance_beaten,
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
    spd.race_class,
    spd.conditions,
    spd.distance,
    spd.distance_full,
    spd.going,
    spd.winning_time,
    spd.horse_type,
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
    spd.owner_id,
    spd.race_id,
    spd.unique_id,
    spd.created_at,
    'historcal'::text AS data_type
   FROM (staging.joined_performance_data spd
     LEFT JOIN public.performance_data pd ON (((spd.unique_id)::text = (pd.unique_id)::text)))
  WHERE ((pd.unique_id IS NULL) AND ((spd.race_date)::text > '2010-01-01'::text));


ALTER VIEW public.missing_performance_data_vw OWNER TO tomwattley;

--
-- Name: race_data; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id integer NOT NULL,
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE public.race_data OWNER TO tomwattley;

--
-- Name: missing_race_data_vw; Type: VIEW; Schema: public; Owner: tomwattley
--

CREATE VIEW public.missing_race_data_vw AS
 SELECT spd.horse_name,
    spd.horse_age,
    spd.jockey_name,
    spd.jockey_claim,
    spd.trainer_name,
    spd.owner_name,
    spd.weight_carried,
    spd.draw,
    spd.finishing_position,
    spd.total_distance_beaten,
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
    spd.race_class,
    spd.conditions,
    spd.distance,
    spd.distance_full,
    spd.going,
    spd.winning_time,
    spd.horse_type,
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
    spd.owner_id,
    spd.race_id,
    spd.unique_id,
    spd.created_at
   FROM (staging.joined_performance_data spd
     LEFT JOIN public.race_data pd ON (((spd.race_id)::text = (pd.race_id)::text)))
  WHERE (pd.race_id IS NULL);


ALTER VIEW public.missing_race_data_vw OWNER TO tomwattley;

--
-- Name: todays_joined_performance_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.todays_joined_performance_data (
    horse_name character varying(132),
    horse_age character varying(16),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    owner_name character varying(132),
    weight_carried character varying(16),
    draw character varying(16),
    finishing_position character varying(16),
    total_distance_beaten character varying(16),
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
    race_class character varying(32),
    conditions character varying(32),
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    winning_time character varying(32),
    horse_type character varying(32),
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
    course_id smallint,
    horse_id integer,
    sire_id integer,
    dam_id integer,
    trainer_id integer,
    jockey_id integer,
    owner_id integer,
    race_id character varying(32),
    unique_id character varying(132),
    created_at timestamp without time zone
);


ALTER TABLE staging.todays_joined_performance_data OWNER TO tomwattley;

--
-- Name: missing_todays_performance_data_vw; Type: VIEW; Schema: public; Owner: tomwattley
--

CREATE VIEW public.missing_todays_performance_data_vw AS
 SELECT horse_name,
    horse_age,
    jockey_name,
    jockey_claim,
    trainer_name,
    owner_name,
    weight_carried,
    draw,
    finishing_position,
    total_distance_beaten,
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
    race_class,
    conditions,
    distance,
    distance_full,
    going,
    winning_time,
    horse_type,
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
    owner_id,
    race_id,
    unique_id,
    created_at,
    'today'::text AS data_type
   FROM staging.todays_joined_performance_data spd;


ALTER VIEW public.missing_todays_performance_data_vw OWNER TO tomwattley;

--
-- Name: missing_todays_race_data_vw; Type: VIEW; Schema: public; Owner: tomwattley
--

CREATE VIEW public.missing_todays_race_data_vw AS
 SELECT DISTINCT ON (spd.race_id) spd.horse_name,
    spd.horse_age,
    spd.jockey_name,
    spd.jockey_claim,
    spd.trainer_name,
    spd.owner_name,
    spd.weight_carried,
    spd.draw,
    spd.finishing_position,
    spd.total_distance_beaten,
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
    spd.race_class,
    spd.conditions,
    spd.distance,
    spd.distance_full,
    spd.going,
    spd.winning_time,
    spd.horse_type,
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
    spd.owner_id,
    spd.race_id,
    spd.unique_id,
    spd.created_at
   FROM (staging.todays_joined_performance_data spd
     LEFT JOIN public.race_data pd ON (((spd.race_id)::text = (pd.race_id)::text)))
  WHERE (pd.race_id IS NULL);


ALTER VIEW public.missing_todays_race_data_vw OWNER TO tomwattley;

--
-- Name: non_uk_ire_race_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.non_uk_ire_race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id integer NOT NULL,
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE public.non_uk_ire_race_data OWNER TO postgres;

--
-- Name: owner; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.owner (
    rp_id character varying(32),
    name text,
    id integer NOT NULL
);


ALTER TABLE public.owner OWNER TO tomwattley;

--
-- Name: owner_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.owner_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.owner_id_seq OWNER TO tomwattley;

--
-- Name: owner_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomwattley
--

ALTER SEQUENCE public.owner_id_seq OWNED BY public.owner.id;


--
-- Name: sire; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.sire (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.sire OWNER TO tomwattley;

--
-- Name: sire_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.sire_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sire_id_seq OWNER TO tomwattley;

--
-- Name: sire_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomwattley
--

ALTER SEQUENCE public.sire_id_seq OWNED BY public.sire.id;


--
-- Name: sire_sire_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.sire_sire_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sire_sire_id_seq OWNER TO tomwattley;

--
-- Name: surface; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.surface (
    name character varying(132),
    id smallint NOT NULL
);


ALTER TABLE public.surface OWNER TO postgres;

--
-- Name: todays_performance_data_mat_vw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.todays_performance_data_mat_vw (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    industry_sp character varying(16),
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    price_change numeric(6,1),
    official_rating smallint,
    ts smallint,
    rpr smallint,
    tfr smallint,
    tfig smallint,
    in_play_high numeric(6,2),
    in_play_low numeric(6,2),
    in_race_comment text,
    tf_comment text,
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    data_type character varying(32),
    created_at timestamp without time zone
);


ALTER TABLE public.todays_performance_data_mat_vw OWNER TO postgres;

--
-- Name: todays_price_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.todays_price_data (
    race_time timestamp without time zone,
    race_date date,
    horse_id integer,
    horse_name character varying(132),
    market_id_win character varying(32),
    market_id_place character varying(32),
    runners_unique_id integer,
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    price_change numeric(6,1)
);


ALTER TABLE public.todays_price_data OWNER TO postgres;

--
-- Name: todays_race_data; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.todays_race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id integer NOT NULL,
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE public.todays_race_data OWNER TO tomwattley;

--
-- Name: trainer; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.trainer (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.trainer OWNER TO tomwattley;

--
-- Name: trainer_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.trainer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainer_id_seq OWNER TO tomwattley;

--
-- Name: trainer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomwattley
--

ALTER SEQUENCE public.trainer_id_seq OWNED BY public.trainer.id;


--
-- Name: trainer_trainer_id_seq; Type: SEQUENCE; Schema: public; Owner: tomwattley
--

CREATE SEQUENCE public.trainer_trainer_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainer_trainer_id_seq OWNER TO tomwattley;

--
-- Name: unioned_performance_data; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
)
PARTITION BY RANGE (race_date);


ALTER TABLE public.unioned_performance_data OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2010; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2010 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2010 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2011; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2011 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2011 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2012; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2012 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2012 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2013; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2013 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2013 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2014; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2014 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2014 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2015; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2015 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2015 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2016; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2016 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2016 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2017; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2017 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2017 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2018; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2018 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2018 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2019; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2019 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2019 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2020; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2020 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2020 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2021; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2021 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2021 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2022; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2022 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2022 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2023; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2023 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2023 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2024; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2024 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2024 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2025; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2025 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2025 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2026; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2026 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2026 OWNER TO tomwattley;

--
-- Name: unioned_performance_data_2027; Type: TABLE; Schema: public; Owner: tomwattley
--

CREATE TABLE public.unioned_performance_data_2027 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
    horse_id integer,
    jockey_id integer,
    trainer_id integer,
    owner_id integer,
    sire_id integer,
    dam_id integer,
    unique_id character varying(132),
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    course_id smallint,
    course character varying(132),
    dam character varying(132),
    sire character varying(132),
    trainer character varying(132),
    jockey character varying(132),
    price_move numeric(6,2),
    created_at timestamp without time zone
);


ALTER TABLE public.unioned_performance_data_2027 OWNER TO tomwattley;

--
-- Name: days_results_links; Type: TABLE; Schema: rp_raw; Owner: tomwattley
--

CREATE TABLE rp_raw.days_results_links (
    date date,
    link_url text
);


ALTER TABLE rp_raw.days_results_links OWNER TO tomwattley;

--
-- Name: days_results_links_errors; Type: TABLE; Schema: rp_raw; Owner: tomwattley
--

CREATE TABLE rp_raw.days_results_links_errors (
    link_url text
);


ALTER TABLE rp_raw.days_results_links_errors OWNER TO tomwattley;

--
-- Name: missing_dates; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.missing_dates AS
 SELECT gs.date
   FROM (generate_series(('2005-01-01'::date)::timestamp without time zone, ((now())::date - '3 days'::interval), '1 day'::interval) gs(date)
     LEFT JOIN ( SELECT DISTINCT days_results_links.date
           FROM rp_raw.days_results_links) distinct_dates ON ((gs.date = distinct_dates.date)))
  WHERE ((distinct_dates.date IS NULL) AND (NOT (gs.date = ANY (ARRAY[('2020-04-06'::date)::timestamp without time zone, ('2008-12-25'::date)::timestamp without time zone, ('2013-03-29'::date)::timestamp without time zone, ('2010-01-06'::date)::timestamp without time zone, ('2008-12-24'::date)::timestamp without time zone, ('2009-02-02'::date)::timestamp without time zone, ('2007-12-25'::date)::timestamp without time zone, ('2007-12-24'::date)::timestamp without time zone, ('2005-12-25'::date)::timestamp without time zone, ('2005-12-24'::date)::timestamp without time zone, ('2009-12-25'::date)::timestamp without time zone, ('2009-12-24'::date)::timestamp without time zone, ('2006-12-25'::date)::timestamp without time zone, ('2006-12-24'::date)::timestamp without time zone, ('2020-04-03'::date)::timestamp without time zone, ('2020-04-02'::date)::timestamp without time zone, ('2015-12-25'::date)::timestamp without time zone, ('2015-12-24'::date)::timestamp without time zone, ('2016-12-25'::date)::timestamp without time zone, ('2016-12-24'::date)::timestamp without time zone, ('2017-12-25'::date)::timestamp without time zone, ('2017-12-24'::date)::timestamp without time zone, ('2018-12-25'::date)::timestamp without time zone, ('2018-12-24'::date)::timestamp without time zone, ('2019-12-25'::date)::timestamp without time zone, ('2019-12-24'::date)::timestamp without time zone, ('2020-12-25'::date)::timestamp without time zone, ('2020-12-24'::date)::timestamp without time zone, ('2021-12-25'::date)::timestamp without time zone, ('2021-12-24'::date)::timestamp without time zone, ('2022-12-25'::date)::timestamp without time zone, ('2022-12-24'::date)::timestamp without time zone, ('2023-12-25'::date)::timestamp without time zone, ('2023-12-24'::date)::timestamp without time zone]))))
  ORDER BY gs.date DESC;


ALTER VIEW rp_raw.missing_dates OWNER TO tomwattley;

--
-- Name: non_uk_ire_performance_data; Type: TABLE; Schema: rp_raw; Owner: postgres
--

CREATE TABLE rp_raw.non_uk_ire_performance_data (
    race_timestamp timestamp without time zone,
    race_date character varying(32),
    course_name character varying(132),
    race_class character varying(132),
    horse_name character varying(132),
    horse_type character varying(32),
    horse_age character varying(16),
    headgear character varying(32),
    conditions character varying(32),
    horse_price character varying(32),
    race_title text,
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    official_rating character varying(32),
    horse_weight character varying(32),
    draw character varying(32),
    country character varying(32),
    surface character varying(32),
    finishing_position character varying(32),
    total_distance_beaten character varying(32),
    ts_value character varying(32),
    rpr_value character varying(32),
    extra_weight numeric(10,2),
    comment text,
    race_time character varying(32),
    currency character varying(32),
    course character varying(132),
    jockey_name character varying(132),
    jockey_claim character varying(32),
    trainer_name character varying(132),
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    owner_name character varying(132),
    horse_id character varying(32),
    trainer_id character varying(32),
    jockey_id character varying(32),
    sire_id character varying(32),
    dam_id character varying(32),
    dams_sire_id character varying(32),
    owner_id character varying(32),
    race_id character varying(32),
    course_id character varying(32),
    meeting_id character varying(132),
    unique_id character varying(132),
    debug_link text,
    created_at timestamp without time zone
);


ALTER TABLE rp_raw.non_uk_ire_performance_data OWNER TO postgres;

--
-- Name: missing_non_uk_ire_links; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.missing_non_uk_ire_links AS
 WITH courses AS (
         SELECT dl.date,
            dl.link_url AS link,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 4) AS course_part
           FROM rp_raw.days_results_links dl
        )
 SELECT DISTINCT cr.link AS link_url,
    cr.date
   FROM (courses cr
     LEFT JOIN rp_raw.non_uk_ire_performance_data pd ON ((cr.link = pd.debug_link)))
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2010-01-01'::date) AND (cr.date < (CURRENT_DATE - '2 days'::interval)) AND (cr.course_part IN ( SELECT course.rp_name
           FROM public.course
          WHERE ((course.country_id)::integer > 2))) AND (NOT (cr.link IN ( SELECT DISTINCT days_results_links_errors.link_url
           FROM rp_raw.days_results_links_errors))));


ALTER VIEW rp_raw.missing_non_uk_ire_links OWNER TO tomwattley;

--
-- Name: void_races; Type: TABLE; Schema: rp_raw; Owner: tomwattley
--

CREATE TABLE rp_raw.void_races (
    link_url text
);


ALTER TABLE rp_raw.void_races OWNER TO tomwattley;

--
-- Name: missing_uk_ire_links; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.missing_uk_ire_links AS
 WITH courses AS (
         SELECT dl.date,
            dl.link_url AS link,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 4) AS course_part
           FROM rp_raw.days_results_links dl
        )
 SELECT DISTINCT cr.link AS link_url,
    cr.date
   FROM (courses cr
     LEFT JOIN rp_raw.performance_data pd ON ((cr.link = pd.debug_link)))
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2010-01-01'::date) AND (cr.date < (CURRENT_DATE - '2 days'::interval)) AND (cr.course_part IN ( SELECT course.rp_name
           FROM public.course
          WHERE ((course.country_id)::integer <= 2))) AND (NOT (cr.link IN ( SELECT void_races.link_url
           FROM rp_raw.void_races))));


ALTER VIEW rp_raw.missing_uk_ire_links OWNER TO tomwattley;

--
-- Name: new_table; Type: TABLE; Schema: rp_raw; Owner: postgres
--

CREATE TABLE rp_raw.new_table (
    race_timestamp timestamp without time zone,
    race_date character varying(32),
    course_name character varying(132),
    race_class character varying(132),
    horse_name character varying(132),
    horse_type character varying(16),
    horse_age character varying(16),
    headgear character varying(16),
    conditions character varying(32),
    horse_price character varying(16),
    race_title text,
    distance character varying(32),
    distance_full character varying(32),
    going character varying(32),
    number_of_runners character varying(32),
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    official_rating character varying(16),
    horse_weight character varying(16),
    draw character varying(16),
    country character varying(16),
    surface character varying(16),
    finishing_position character varying(16),
    total_distance_beaten character varying(32),
    ts_value character varying(16),
    rpr_value character varying(16),
    extra_weight numeric(10,2),
    comment text,
    race_time character varying(32),
    currency character varying(16),
    course character varying(132),
    jockey_name character varying(132),
    jockey_claim character varying(16),
    trainer_name character varying(132),
    sire_name character varying(132),
    dam_name character varying(132),
    dams_sire character varying(132),
    owner_name character varying(132),
    horse_id character varying(32),
    trainer_id character varying(32),
    jockey_id character varying(32),
    sire_id character varying(32),
    dam_id character varying(32),
    dams_sire_id character varying(32),
    owner_id character varying(32),
    race_id character varying(32),
    course_id character varying(32),
    meeting_id character varying(132),
    unique_id character varying(132),
    debug_link text,
    created_at timestamp without time zone
);


ALTER TABLE rp_raw.new_table OWNER TO postgres;

--
-- Name: todays_races; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.todays_races AS
 SELECT DISTINCT race_date,
    course,
    race_timestamp
   FROM rp_raw.todays_performance_data
  WHERE ((race_date)::date >= CURRENT_DATE)
  ORDER BY race_date, course, race_timestamp;


ALTER VIEW rp_raw.todays_races OWNER TO tomwattley;

--
-- Name: unmatched_dams; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.unmatched_dams AS
 WITH historical_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.dam ON (((pd.dam_id)::text = (dam.rp_id)::text)))
          WHERE (dam.rp_id IS NULL)
        ), present_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.todays_performance_data pd
             LEFT JOIN public.dam ON (((pd.dam_id)::text = (dam.rp_id)::text)))
          WHERE ((dam.rp_id IS NULL) AND ((pd.race_date)::date >= CURRENT_DATE))
        ), non_uk_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.dam ON (((pd.dam_id)::text = (dam.rp_id)::text)))
          WHERE (dam.rp_id IS NULL)
        ), unioned_data AS (
         SELECT historical_data.horse_name,
            historical_data.horse_id,
            historical_data.horse_age,
            historical_data.jockey_id,
            historical_data.jockey_name,
            historical_data.trainer_id,
            historical_data.trainer_name,
            historical_data.sire_name,
            historical_data.sire_id,
            historical_data.dam_name,
            historical_data.dam_id,
            historical_data.race_timestamp,
            historical_data.course_id,
            historical_data.unique_id,
            historical_data.race_date,
            historical_data.debug_link
           FROM historical_data
        UNION
         SELECT present_data.horse_name,
            present_data.horse_id,
            present_data.horse_age,
            present_data.jockey_id,
            present_data.jockey_name,
            present_data.trainer_id,
            present_data.trainer_name,
            present_data.sire_name,
            present_data.sire_id,
            present_data.dam_name,
            present_data.dam_id,
            present_data.race_timestamp,
            present_data.course_id,
            present_data.unique_id,
            present_data.race_date,
            present_data.debug_link
           FROM present_data
        )
 SELECT u.horse_name,
    u.horse_id,
    u.horse_age,
    u.jockey_id,
    u.jockey_name,
    u.trainer_id,
    u.trainer_name,
    u.sire_name,
    u.sire_id,
    u.dam_name,
    u.dam_id,
    u.race_timestamp,
    u.unique_id,
    u.race_date,
    u.debug_link,
    c.id AS course_id
   FROM (unioned_data u
     LEFT JOIN public.course c ON (((u.course_id)::text = (c.rp_id)::text)));


ALTER VIEW rp_raw.unmatched_dams OWNER TO tomwattley;

--
-- Name: unmatched_horses; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.unmatched_horses AS
 WITH historical_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.horse ON (((pd.horse_id)::text = (horse.rp_id)::text)))
          WHERE (horse.rp_id IS NULL)
        ), present_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.todays_performance_data pd
             LEFT JOIN public.horse ON (((pd.horse_id)::text = (horse.rp_id)::text)))
          WHERE ((horse.rp_id IS NULL) AND ((pd.race_date)::date >= CURRENT_DATE))
        ), non_uk_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.non_uk_ire_performance_data pd
             LEFT JOIN public.horse ON (((pd.horse_id)::text = (horse.rp_id)::text)))
          WHERE (horse.rp_id IS NULL)
        ), unioned_data AS (
         SELECT historical_data.horse_name,
            historical_data.horse_id,
            historical_data.horse_age,
            historical_data.jockey_id,
            historical_data.jockey_name,
            historical_data.trainer_id,
            historical_data.trainer_name,
            historical_data.sire_name,
            historical_data.sire_id,
            historical_data.dam_name,
            historical_data.dam_id,
            historical_data.race_timestamp,
            historical_data.course_id,
            historical_data.unique_id,
            historical_data.race_date,
            historical_data.debug_link
           FROM historical_data
        UNION
         SELECT present_data.horse_name,
            present_data.horse_id,
            present_data.horse_age,
            present_data.jockey_id,
            present_data.jockey_name,
            present_data.trainer_id,
            present_data.trainer_name,
            present_data.sire_name,
            present_data.sire_id,
            present_data.dam_name,
            present_data.dam_id,
            present_data.race_timestamp,
            present_data.course_id,
            present_data.unique_id,
            present_data.race_date,
            present_data.debug_link
           FROM present_data
        )
 SELECT u.horse_name,
    u.horse_id,
    u.horse_age,
    u.jockey_id,
    u.jockey_name,
    u.trainer_id,
    u.trainer_name,
    u.sire_name,
    u.sire_id,
    u.dam_name,
    u.dam_id,
    u.race_timestamp,
    u.unique_id,
    u.race_date,
    u.debug_link,
    c.id AS course_id
   FROM (unioned_data u
     LEFT JOIN public.course c ON (((u.course_id)::text = (c.rp_id)::text)));


ALTER VIEW rp_raw.unmatched_horses OWNER TO tomwattley;

--
-- Name: unmatched_jockeys; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.unmatched_jockeys AS
 WITH historical_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.jockey ON (((pd.jockey_id)::text = (jockey.rp_id)::text)))
          WHERE (jockey.rp_id IS NULL)
        ), present_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.todays_performance_data pd
             LEFT JOIN public.jockey ON (((pd.jockey_id)::text = (jockey.rp_id)::text)))
          WHERE ((jockey.rp_id IS NULL) AND ((pd.race_date)::date >= CURRENT_DATE))
        ), non_uk_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.non_uk_ire_performance_data pd
             LEFT JOIN public.jockey ON (((pd.jockey_id)::text = (jockey.rp_id)::text)))
          WHERE (jockey.rp_id IS NULL)
        ), unioned_data AS (
         SELECT historical_data.horse_name,
            historical_data.horse_id,
            historical_data.horse_age,
            historical_data.jockey_id,
            historical_data.jockey_name,
            historical_data.trainer_id,
            historical_data.trainer_name,
            historical_data.sire_name,
            historical_data.sire_id,
            historical_data.dam_name,
            historical_data.dam_id,
            historical_data.race_timestamp,
            historical_data.course_id,
            historical_data.unique_id,
            historical_data.race_date,
            historical_data.debug_link
           FROM historical_data
        UNION
         SELECT present_data.horse_name,
            present_data.horse_id,
            present_data.horse_age,
            present_data.jockey_id,
            present_data.jockey_name,
            present_data.trainer_id,
            present_data.trainer_name,
            present_data.sire_name,
            present_data.sire_id,
            present_data.dam_name,
            present_data.dam_id,
            present_data.race_timestamp,
            present_data.course_id,
            present_data.unique_id,
            present_data.race_date,
            present_data.debug_link
           FROM present_data
        )
 SELECT u.horse_name,
    u.horse_id,
    u.horse_age,
    u.jockey_id,
    u.jockey_name,
    u.trainer_id,
    u.trainer_name,
    u.sire_name,
    u.sire_id,
    u.dam_name,
    u.dam_id,
    u.race_timestamp,
    u.unique_id,
    u.race_date,
    u.debug_link,
    c.id AS course_id
   FROM (unioned_data u
     LEFT JOIN public.course c ON (((u.course_id)::text = (c.rp_id)::text)));


ALTER VIEW rp_raw.unmatched_jockeys OWNER TO tomwattley;

--
-- Name: unmatched_owners; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.unmatched_owners AS
 WITH historical_data AS (
         SELECT pd.owner_name,
            pd.owner_id
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.owner ON (((pd.owner_id)::text = (owner.rp_id)::text)))
          WHERE (owner.rp_id IS NULL)
        ), present_data AS (
         SELECT pd.owner_name,
            pd.owner_id
           FROM (rp_raw.todays_performance_data pd
             LEFT JOIN public.owner ON (((pd.owner_id)::text = (owner.rp_id)::text)))
          WHERE ((owner.rp_id IS NULL) AND ((pd.race_date)::date >= CURRENT_DATE))
        ), non_uk_data AS (
         SELECT pd.owner_name,
            pd.owner_id
           FROM (rp_raw.non_uk_ire_performance_data pd
             LEFT JOIN public.owner ON (((pd.owner_id)::text = (owner.rp_id)::text)))
          WHERE (owner.rp_id IS NULL)
        ), unioned_data AS (
         SELECT historical_data.owner_name,
            historical_data.owner_id
           FROM historical_data
        UNION
         SELECT present_data.owner_name,
            present_data.owner_id
           FROM present_data
        UNION
         SELECT non_uk_data.owner_name,
            non_uk_data.owner_id
           FROM non_uk_data
        )
 SELECT owner_name AS name,
    owner_id AS rp_id
   FROM unioned_data u;


ALTER VIEW rp_raw.unmatched_owners OWNER TO tomwattley;

--
-- Name: unmatched_sires; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.unmatched_sires AS
 WITH historical_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.sire ON (((pd.sire_id)::text = (sire.rp_id)::text)))
          WHERE (sire.rp_id IS NULL)
        ), present_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.todays_performance_data pd
             LEFT JOIN public.sire ON (((pd.sire_id)::text = (sire.rp_id)::text)))
          WHERE ((sire.rp_id IS NULL) AND ((pd.race_date)::date >= CURRENT_DATE))
        ), non_uk_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.non_uk_ire_performance_data pd
             LEFT JOIN public.sire ON (((pd.sire_id)::text = (sire.rp_id)::text)))
          WHERE (sire.rp_id IS NULL)
        ), unioned_data AS (
         SELECT historical_data.horse_name,
            historical_data.horse_id,
            historical_data.horse_age,
            historical_data.jockey_id,
            historical_data.jockey_name,
            historical_data.trainer_id,
            historical_data.trainer_name,
            historical_data.sire_name,
            historical_data.sire_id,
            historical_data.dam_name,
            historical_data.dam_id,
            historical_data.race_timestamp,
            historical_data.course_id,
            historical_data.unique_id,
            historical_data.race_date,
            historical_data.debug_link
           FROM historical_data
        UNION
         SELECT present_data.horse_name,
            present_data.horse_id,
            present_data.horse_age,
            present_data.jockey_id,
            present_data.jockey_name,
            present_data.trainer_id,
            present_data.trainer_name,
            present_data.sire_name,
            present_data.sire_id,
            present_data.dam_name,
            present_data.dam_id,
            present_data.race_timestamp,
            present_data.course_id,
            present_data.unique_id,
            present_data.race_date,
            present_data.debug_link
           FROM present_data
        )
 SELECT u.horse_name,
    u.horse_id,
    u.horse_age,
    u.jockey_id,
    u.jockey_name,
    u.trainer_id,
    u.trainer_name,
    u.sire_name,
    u.sire_id,
    u.dam_name,
    u.dam_id,
    u.race_timestamp,
    u.unique_id,
    u.race_date,
    u.debug_link,
    c.id AS course_id
   FROM (unioned_data u
     LEFT JOIN public.course c ON (((u.course_id)::text = (c.rp_id)::text)));


ALTER VIEW rp_raw.unmatched_sires OWNER TO tomwattley;

--
-- Name: unmatched_trainers; Type: VIEW; Schema: rp_raw; Owner: tomwattley
--

CREATE VIEW rp_raw.unmatched_trainers AS
 WITH historical_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.trainer ON (((pd.trainer_id)::text = (trainer.rp_id)::text)))
          WHERE (trainer.rp_id IS NULL)
        ), present_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.todays_performance_data pd
             LEFT JOIN public.trainer ON (((pd.trainer_id)::text = (trainer.rp_id)::text)))
          WHERE ((trainer.rp_id IS NULL) AND ((pd.race_date)::date >= CURRENT_DATE))
        ), non_uk_data AS (
         SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.course_id,
            pd.unique_id,
            pd.race_date,
            pd.debug_link
           FROM (rp_raw.non_uk_ire_performance_data pd
             LEFT JOIN public.trainer ON (((pd.trainer_id)::text = (trainer.rp_id)::text)))
          WHERE (trainer.rp_id IS NULL)
        ), unioned_data AS (
         SELECT historical_data.horse_name,
            historical_data.horse_id,
            historical_data.horse_age,
            historical_data.jockey_id,
            historical_data.jockey_name,
            historical_data.trainer_id,
            historical_data.trainer_name,
            historical_data.sire_name,
            historical_data.sire_id,
            historical_data.dam_name,
            historical_data.dam_id,
            historical_data.race_timestamp,
            historical_data.course_id,
            historical_data.unique_id,
            historical_data.race_date,
            historical_data.debug_link
           FROM historical_data
        UNION
         SELECT present_data.horse_name,
            present_data.horse_id,
            present_data.horse_age,
            present_data.jockey_id,
            present_data.jockey_name,
            present_data.trainer_id,
            present_data.trainer_name,
            present_data.sire_name,
            present_data.sire_id,
            present_data.dam_name,
            present_data.dam_id,
            present_data.race_timestamp,
            present_data.course_id,
            present_data.unique_id,
            present_data.race_date,
            present_data.debug_link
           FROM present_data
        )
 SELECT u.horse_name,
    u.horse_id,
    u.horse_age,
    u.jockey_id,
    u.jockey_name,
    u.trainer_id,
    u.trainer_name,
    u.sire_name,
    u.sire_id,
    u.dam_name,
    u.dam_id,
    u.race_timestamp,
    u.unique_id,
    u.race_date,
    u.debug_link,
    c.id AS course_id
   FROM (unioned_data u
     LEFT JOIN public.course c ON (((u.course_id)::text = (c.rp_id)::text)));


ALTER VIEW rp_raw.unmatched_trainers OWNER TO tomwattley;

--
-- Name: missing_performance_data_vw; Type: VIEW; Schema: staging; Owner: tomwattley
--

CREATE VIEW staging.missing_performance_data_vw AS
 SELECT race_timestamp,
    race_date,
    course_name,
    race_class,
    horse_name,
    horse_type,
    horse_age,
    headgear,
    conditions,
    horse_price,
    race_title,
    distance,
    distance_full,
    going,
    number_of_runners,
    total_prize_money,
    first_place_prize_money,
    winning_time,
    official_rating,
    horse_weight,
    draw,
    country,
    surface,
    finishing_position,
    total_distance_beaten,
    ts_value,
    rpr_value,
    extra_weight,
    comment,
    race_time,
    currency,
    course,
    jockey_name,
    jockey_claim,
    trainer_name,
    sire_name,
    dam_name,
    dams_sire,
    owner_name,
    horse_id,
    trainer_id,
    jockey_id,
    sire_id,
    dam_id,
    dams_sire_id,
    owner_id,
    race_id,
    course_id,
    meeting_id,
    unique_id,
    debug_link,
    created_at,
    rn
   FROM ( SELECT rp.race_timestamp,
            rp.race_date,
            rp.course_name,
            rp.race_class,
            rp.horse_name,
            rp.horse_type,
            rp.horse_age,
            rp.headgear,
            rp.conditions,
            rp.horse_price,
            rp.race_title,
            rp.distance,
            rp.distance_full,
            rp.going,
            rp.number_of_runners,
            rp.total_prize_money,
            rp.first_place_prize_money,
            rp.winning_time,
            rp.official_rating,
            rp.horse_weight,
            rp.draw,
            rp.country,
            rp.surface,
            rp.finishing_position,
            rp.total_distance_beaten,
            rp.ts_value,
            rp.rpr_value,
            rp.extra_weight,
            rp.comment,
            rp.race_time,
            rp.currency,
            rp.course,
            rp.jockey_name,
            rp.jockey_claim,
            rp.trainer_name,
            rp.sire_name,
            rp.dam_name,
            rp.dams_sire,
            rp.owner_name,
            rp.horse_id,
            rp.trainer_id,
            rp.jockey_id,
            rp.sire_id,
            rp.dam_id,
            rp.dams_sire_id,
            rp.owner_id,
            rp.race_id,
            rp.course_id,
            rp.meeting_id,
            rp.unique_id,
            rp.debug_link,
            rp.created_at,
            row_number() OVER (PARTITION BY rp.unique_id ORDER BY rp.created_at DESC) AS rn
           FROM (rp_raw.performance_data rp
             LEFT JOIN staging.joined_performance_data sjpd ON (((rp.unique_id)::text = (sjpd.unique_id)::text)))
          WHERE ((sjpd.unique_id IS NULL) AND (rp.race_timestamp <> '2018-12-02 15:05:00'::timestamp without time zone) AND (rp.race_timestamp > '2010-01-01 00:00:00'::timestamp without time zone))) sub
  WHERE (rn = 1);


ALTER VIEW staging.missing_performance_data_vw OWNER TO tomwattley;

--
-- Name: todays_transformed_performance_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.todays_transformed_performance_data (
    race_time timestamp without time zone,
    race_date date,
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
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
    tfr_view character varying(16),
    race_id integer,
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


ALTER TABLE staging.todays_transformed_performance_data OWNER TO tomwattley;

--
-- Name: todays_transformed_price_data; Type: TABLE; Schema: staging; Owner: postgres
--

CREATE TABLE staging.todays_transformed_price_data (
    race_time timestamp without time zone,
    race_date date,
    horse_id integer,
    horse_name character varying(132),
    market_id_win character varying(32),
    market_id_place character varying(32),
    runners_unique_id integer,
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    price_change numeric(6,1)
);


ALTER TABLE staging.todays_transformed_price_data OWNER TO postgres;

--
-- Name: todays_transformed_race_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.todays_transformed_race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id integer,
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE staging.todays_transformed_race_data OWNER TO tomwattley;

--
-- Name: transformed_non_uk_ire_performance_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.transformed_non_uk_ire_performance_data (
    race_time timestamp without time zone,
    race_date date,
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    draw integer,
    headgear character varying(64),
    weight_carried character varying(64),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    industry_sp character varying(64),
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
    tfr_view character varying(64),
    race_id integer,
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


ALTER TABLE staging.transformed_non_uk_ire_performance_data OWNER TO tomwattley;

--
-- Name: transformed_non_uk_ire_race_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.transformed_non_uk_ire_race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id integer,
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE staging.transformed_non_uk_ire_race_data OWNER TO tomwattley;

--
-- Name: transformed_race_data; Type: TABLE; Schema: staging; Owner: tomwattley
--

CREATE TABLE staging.transformed_race_data (
    race_time timestamp without time zone,
    race_date date,
    race_title character varying(132),
    race_type character varying(32),
    race_class smallint,
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
    total_prize_money integer,
    first_place_prize_money integer,
    winning_time character varying(32),
    time_seconds numeric(10,2),
    relative_time numeric(10,2),
    relative_to_standard character varying(16),
    country character varying(64),
    main_race_comment text,
    meeting_id character varying(132),
    race_id integer,
    course_id smallint,
    created_at timestamp without time zone
);


ALTER TABLE staging.transformed_race_data OWNER TO tomwattley;

--
-- Name: days_results_links; Type: TABLE; Schema: tf_raw; Owner: tomwattley
--

CREATE TABLE tf_raw.days_results_links (
    date date,
    link_url text
);


ALTER TABLE tf_raw.days_results_links OWNER TO tomwattley;

--
-- Name: days_results_links_errors; Type: TABLE; Schema: tf_raw; Owner: tomwattley
--

CREATE TABLE tf_raw.days_results_links_errors (
    link_url text
);


ALTER TABLE tf_raw.days_results_links_errors OWNER TO tomwattley;

--
-- Name: missing_dates; Type: VIEW; Schema: tf_raw; Owner: tomwattley
--

CREATE VIEW tf_raw.missing_dates AS
 SELECT gs.date
   FROM (generate_series(('2005-01-01'::date)::timestamp without time zone, ((now())::date - '3 days'::interval), '1 day'::interval) gs(date)
     LEFT JOIN ( SELECT DISTINCT days_results_links.date
           FROM tf_raw.days_results_links) distinct_dates ON ((gs.date = distinct_dates.date)))
  WHERE ((distinct_dates.date IS NULL) AND (NOT (gs.date = ANY (ARRAY[('2005-03-25'::date)::timestamp without time zone, ('2009-12-23'::date)::timestamp without time zone, ('2020-04-06'::date)::timestamp without time zone, ('2008-12-25'::date)::timestamp without time zone, ('2013-03-29'::date)::timestamp without time zone, ('2010-01-06'::date)::timestamp without time zone, ('2008-12-24'::date)::timestamp without time zone, ('2009-02-02'::date)::timestamp without time zone, ('2007-12-25'::date)::timestamp without time zone, ('2007-12-24'::date)::timestamp without time zone, ('2005-12-25'::date)::timestamp without time zone, ('2005-12-24'::date)::timestamp without time zone, ('2009-12-25'::date)::timestamp without time zone, ('2009-12-24'::date)::timestamp without time zone, ('2006-12-25'::date)::timestamp without time zone, ('2006-12-24'::date)::timestamp without time zone, ('2020-04-03'::date)::timestamp without time zone, ('2020-04-02'::date)::timestamp without time zone, ('2015-12-25'::date)::timestamp without time zone, ('2015-12-24'::date)::timestamp without time zone, ('2016-12-25'::date)::timestamp without time zone, ('2016-12-24'::date)::timestamp without time zone, ('2017-12-25'::date)::timestamp without time zone, ('2017-12-24'::date)::timestamp without time zone, ('2018-12-25'::date)::timestamp without time zone, ('2018-12-24'::date)::timestamp without time zone, ('2019-12-25'::date)::timestamp without time zone, ('2019-12-24'::date)::timestamp without time zone, ('2020-12-25'::date)::timestamp without time zone, ('2020-12-24'::date)::timestamp without time zone, ('2021-12-25'::date)::timestamp without time zone, ('2021-12-24'::date)::timestamp without time zone, ('2022-12-25'::date)::timestamp without time zone, ('2022-12-24'::date)::timestamp without time zone, ('2023-12-25'::date)::timestamp without time zone, ('2023-12-24'::date)::timestamp without time zone]))))
  ORDER BY gs.date DESC;


ALTER VIEW tf_raw.missing_dates OWNER TO tomwattley;

--
-- Name: non_uk_ire_performance_data; Type: TABLE; Schema: tf_raw; Owner: tomwattley
--

CREATE TABLE tf_raw.non_uk_ire_performance_data (
    tf_rating character varying(64),
    tf_speed_figure character varying(64),
    draw character varying(64),
    trainer_name character varying(132),
    trainer_id character varying(32),
    jockey_name character varying(132),
    jockey_id character varying(32),
    sire_name character varying(132),
    sire_id character varying(32),
    dam_name character varying(132),
    dam_id character varying(32),
    finishing_position character varying(64),
    horse_name character varying(132),
    horse_id character varying(32),
    horse_name_link character varying(132),
    horse_age character varying(64),
    equipment character varying(64),
    official_rating character varying(64),
    fractional_price character varying(64),
    betfair_win_sp character varying(64),
    betfair_place_sp character varying(64),
    in_play_prices character varying(64),
    tf_comment text,
    course text,
    race_date character varying(32),
    race_time character varying(32),
    race_timestamp timestamp without time zone,
    course_id character varying(32),
    race text,
    race_id character varying(132),
    distance character varying(32),
    going character varying(64),
    prize character varying(64),
    hcap_range character varying(64),
    age_range character varying(64),
    race_type character varying(64),
    main_race_comment text,
    debug_link text,
    created_at timestamp without time zone,
    unique_id character varying(132)
);


ALTER TABLE tf_raw.non_uk_ire_performance_data OWNER TO tomwattley;

--
-- Name: missing_non_uk_ire_links; Type: VIEW; Schema: tf_raw; Owner: postgres
--

CREATE VIEW tf_raw.missing_non_uk_ire_links AS
 WITH rp_links AS (
         SELECT dl.date,
            dl.link_url AS link,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 4) AS course_part,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 3) AS course_id
           FROM rp_raw.days_results_links dl
        ), rp_course_dates AS (
         SELECT DISTINCT ON (l.date, c.tf_id) l.date,
            c.tf_id
           FROM (rp_links l
             LEFT JOIN public.course c ON ((l.course_id = (c.rp_id)::text)))
          WHERE (((c.country_id)::integer > 2) AND (l.date > '2010-01-01'::date))
        ), tf_courses AS (
         SELECT dl.date,
            dl.link_url AS link,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 4) AS course_part,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 7) AS course_id
           FROM tf_raw.days_results_links dl
        ), matching_tf_data AS (
         SELECT tf.date,
            tf.link AS link_url
           FROM (tf_courses tf
             LEFT JOIN rp_course_dates rp ON (((tf.date = rp.date) AND (tf.course_id = (rp.tf_id)::text))))
          WHERE (rp.date IS NOT NULL)
          ORDER BY tf.date, tf.course_part
        )
 SELECT mtd.date,
    mtd.link_url
   FROM (matching_tf_data mtd
     LEFT JOIN tf_raw.non_uk_ire_performance_data pd ON ((mtd.link_url = pd.debug_link)))
  WHERE ((pd.debug_link IS NULL) AND (NOT (mtd.link_url IN ( SELECT DISTINCT days_results_links_errors.link_url
           FROM tf_raw.days_results_links_errors))));


ALTER VIEW tf_raw.missing_non_uk_ire_links OWNER TO postgres;

--
-- Name: missing_uk_ire_links; Type: VIEW; Schema: tf_raw; Owner: postgres
--

CREATE VIEW tf_raw.missing_uk_ire_links AS
 WITH courses AS (
         SELECT dl.date,
            dl.link_url AS link,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 4) AS course_part
           FROM tf_raw.days_results_links dl
        )
 SELECT DISTINCT cr.link AS link_url,
    cr.date
   FROM (courses cr
     LEFT JOIN tf_raw.performance_data pd ON ((cr.link = pd.debug_link)))
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2010-01-01'::date) AND (cr.date < (CURRENT_DATE - '2 days'::interval)) AND (cr.course_part IN ( SELECT course.tf_name
           FROM public.course
          WHERE ((course.country_id)::integer <= 2))));


ALTER VIEW tf_raw.missing_uk_ire_links OWNER TO postgres;

--
-- Name: rp_course_dates; Type: VIEW; Schema: tf_raw; Owner: postgres
--

CREATE VIEW tf_raw.rp_course_dates AS
 WITH links AS (
         SELECT dl.date,
            dl.link_url AS link,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 4) AS course_part,
            split_part(split_part(dl.link_url, '://'::text, 2), '/'::text, 3) AS course_id
           FROM rp_raw.days_results_links dl
        )
 SELECT DISTINCT ON (l.date, c.tf_id) l.date,
    c.tf_id
   FROM (links l
     LEFT JOIN public.course c ON ((l.course_id = (c.rp_id)::text)))
  WHERE (((c.country_id)::integer > 2) AND (l.date > '2010-01-01'::date));


ALTER VIEW tf_raw.rp_course_dates OWNER TO postgres;

--
-- Name: unioned_performance_data_2010; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2010 FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');


--
-- Name: unioned_performance_data_2011; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2011 FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');


--
-- Name: unioned_performance_data_2012; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2012 FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');


--
-- Name: unioned_performance_data_2013; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2013 FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');


--
-- Name: unioned_performance_data_2014; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2014 FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');


--
-- Name: unioned_performance_data_2015; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2015 FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');


--
-- Name: unioned_performance_data_2016; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2016 FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');


--
-- Name: unioned_performance_data_2017; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2017 FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');


--
-- Name: unioned_performance_data_2018; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2018 FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');


--
-- Name: unioned_performance_data_2019; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2019 FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');


--
-- Name: unioned_performance_data_2020; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2020 FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');


--
-- Name: unioned_performance_data_2021; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2021 FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');


--
-- Name: unioned_performance_data_2022; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2022 FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');


--
-- Name: unioned_performance_data_2023; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2023 FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');


--
-- Name: unioned_performance_data_2024; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');


--
-- Name: unioned_performance_data_2025; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2025 FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');


--
-- Name: unioned_performance_data_2026; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');


--
-- Name: unioned_performance_data_2027; Type: TABLE ATTACH; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2027 FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');


--
-- Name: dam id; Type: DEFAULT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.dam ALTER COLUMN id SET DEFAULT nextval('public.dam_id_seq'::regclass);


--
-- Name: horse id; Type: DEFAULT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.horse ALTER COLUMN id SET DEFAULT nextval('public.horse_id_seq'::regclass);


--
-- Name: jockey id; Type: DEFAULT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.jockey ALTER COLUMN id SET DEFAULT nextval('public.jockey_id_seq'::regclass);


--
-- Name: owner id; Type: DEFAULT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.owner ALTER COLUMN id SET DEFAULT nextval('public.owner_id_seq'::regclass);


--
-- Name: sire id; Type: DEFAULT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.sire ALTER COLUMN id SET DEFAULT nextval('public.sire_id_seq'::regclass);


--
-- Name: trainer id; Type: DEFAULT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.trainer ALTER COLUMN id SET DEFAULT nextval('public.trainer_id_seq'::regclass);


--
-- Name: selections_info betting_unq_id; Type: CONSTRAINT; Schema: betting; Owner: postgres
--

ALTER TABLE ONLY betting.selections_info
    ADD CONSTRAINT betting_unq_id UNIQUE (unique_id);


--
-- Name: dam dam_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.dam
    ADD CONSTRAINT dam_pkey PRIMARY KEY (id);


--
-- Name: historical_price_data historical_price_data_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historical_price_data
    ADD CONSTRAINT historical_price_data_pkey PRIMARY KEY (unique_id, bf_unique_id);


--
-- Name: horse horse_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.horse
    ADD CONSTRAINT horse_pkey PRIMARY KEY (id);


--
-- Name: jockey jockey_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.jockey
    ADD CONSTRAINT jockey_pkey PRIMARY KEY (id);


--
-- Name: owner owner_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.owner
    ADD CONSTRAINT owner_pkey PRIMARY KEY (id);


--
-- Name: non_uk_ire_race_data race_data_non_uk_ire_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.non_uk_ire_race_data
    ADD CONSTRAINT race_data_non_uk_ire_pkey PRIMARY KEY (race_id);


--
-- Name: race_data race_data_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.race_data
    ADD CONSTRAINT race_data_pkey PRIMARY KEY (race_id);


--
-- Name: owner rp_owner_unique_id; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.owner
    ADD CONSTRAINT rp_owner_unique_id UNIQUE (rp_id, name);


--
-- Name: dam rp_tf_dam_unique_id; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.dam
    ADD CONSTRAINT rp_tf_dam_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: horse rp_tf_horse_unique_id; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.horse
    ADD CONSTRAINT rp_tf_horse_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: jockey rp_tf_jockey_unique_id; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.jockey
    ADD CONSTRAINT rp_tf_jockey_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: sire rp_tf_sire_unique_id; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.sire
    ADD CONSTRAINT rp_tf_sire_unique_id UNIQUE (rp_id, tf_id);


--
-- Name: sire rp_tf_sire_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.sire
    ADD CONSTRAINT rp_tf_sire_unique_id_v2 UNIQUE (rp_id, tf_id);


--
-- Name: trainer rp_tf_trainer_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.trainer
    ADD CONSTRAINT rp_tf_trainer_unique_id_v2 UNIQUE (rp_id, tf_id);


--
-- Name: sire sire_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.sire
    ADD CONSTRAINT sire_pkey PRIMARY KEY (id);


--
-- Name: todays_race_data todays_race_data_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.todays_race_data
    ADD CONSTRAINT todays_race_data_pkey PRIMARY KEY (race_id);


--
-- Name: todays_performance_data todays_unique_id_pd; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.todays_performance_data
    ADD CONSTRAINT todays_unique_id_pd UNIQUE (unique_id);


--
-- Name: trainer trainer_pkey; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.trainer
    ADD CONSTRAINT trainer_pkey PRIMARY KEY (id);


--
-- Name: unioned_performance_data unioned_unq_id_cns; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data
    ADD CONSTRAINT unioned_unq_id_cns UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2010 unioned_performance_data_2010_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2010
    ADD CONSTRAINT unioned_performance_data_2010_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2011 unioned_performance_data_2011_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2011
    ADD CONSTRAINT unioned_performance_data_2011_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2012 unioned_performance_data_2012_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2012
    ADD CONSTRAINT unioned_performance_data_2012_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2013 unioned_performance_data_2013_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2013
    ADD CONSTRAINT unioned_performance_data_2013_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2014 unioned_performance_data_2014_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2014
    ADD CONSTRAINT unioned_performance_data_2014_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2015 unioned_performance_data_2015_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2015
    ADD CONSTRAINT unioned_performance_data_2015_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2016 unioned_performance_data_2016_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2016
    ADD CONSTRAINT unioned_performance_data_2016_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2017 unioned_performance_data_2017_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2017
    ADD CONSTRAINT unioned_performance_data_2017_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2018 unioned_performance_data_2018_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2018
    ADD CONSTRAINT unioned_performance_data_2018_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2019 unioned_performance_data_2019_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2019
    ADD CONSTRAINT unioned_performance_data_2019_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2020 unioned_performance_data_2020_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2020
    ADD CONSTRAINT unioned_performance_data_2020_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2021 unioned_performance_data_2021_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2021
    ADD CONSTRAINT unioned_performance_data_2021_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2022 unioned_performance_data_2022_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2022
    ADD CONSTRAINT unioned_performance_data_2022_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2023 unioned_performance_data_2023_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2023
    ADD CONSTRAINT unioned_performance_data_2023_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2024 unioned_performance_data_2024_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2024
    ADD CONSTRAINT unioned_performance_data_2024_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2025 unioned_performance_data_2025_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2025
    ADD CONSTRAINT unioned_performance_data_2025_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2026 unioned_performance_data_2026_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2026
    ADD CONSTRAINT unioned_performance_data_2026_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: unioned_performance_data_2027 unioned_performance_data_2027_unique_id_race_date_key; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.unioned_performance_data_2027
    ADD CONSTRAINT unioned_performance_data_2027_unique_id_race_date_key UNIQUE (unique_id, race_date);


--
-- Name: todays_price_data unique_horse_id; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.todays_price_data
    ADD CONSTRAINT unique_horse_id UNIQUE (horse_id);


--
-- Name: non_uk_ire_performance_data unique_id_non_uk_ire_pd; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.non_uk_ire_performance_data
    ADD CONSTRAINT unique_id_non_uk_ire_pd UNIQUE (unique_id);


--
-- Name: performance_data unique_id_pd; Type: CONSTRAINT; Schema: public; Owner: tomwattley
--

ALTER TABLE ONLY public.performance_data
    ADD CONSTRAINT unique_id_pd UNIQUE (unique_id);


--
-- Name: transformed_non_uk_ire_race_data race_id_stg_non_uk_ire_tns; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.transformed_non_uk_ire_race_data
    ADD CONSTRAINT race_id_stg_non_uk_ire_tns UNIQUE (race_id);


--
-- Name: transformed_race_data race_id_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.transformed_race_data
    ADD CONSTRAINT race_id_stg_tns UNIQUE (race_id);


--
-- Name: todays_transformed_race_data race_id_todays_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.todays_transformed_race_data
    ADD CONSTRAINT race_id_todays_stg_tns UNIQUE (race_id);


--
-- Name: joined_performance_data stg_jnd_unique_id; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.joined_performance_data
    ADD CONSTRAINT stg_jnd_unique_id UNIQUE (unique_id);


--
-- Name: joined_non_uk_ire_performance_data stg_non_uk_ire_jnd_unique_id; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.joined_non_uk_ire_performance_data
    ADD CONSTRAINT stg_non_uk_ire_jnd_unique_id UNIQUE (unique_id);


--
-- Name: transformed_non_uk_ire_performance_data unique_id_stg_non_uk_ire_tns; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.transformed_non_uk_ire_performance_data
    ADD CONSTRAINT unique_id_stg_non_uk_ire_tns UNIQUE (unique_id);


--
-- Name: transformed_performance_data unique_id_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.transformed_performance_data
    ADD CONSTRAINT unique_id_stg_tns UNIQUE (unique_id);


--
-- Name: todays_transformed_performance_data unique_id_todays_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: tomwattley
--

ALTER TABLE ONLY staging.todays_transformed_performance_data
    ADD CONSTRAINT unique_id_todays_stg_tns UNIQUE (unique_id);


--
-- Name: idx_course_rp_course_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_course_rp_course_id ON public.course USING btree (rp_id);


--
-- Name: idx_course_tf_course_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_course_tf_course_id ON public.course USING btree (tf_id);


--
-- Name: idx_dam_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_dam_id ON public.dam USING btree (rp_id);


--
-- Name: idx_dam_name; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_dam_name ON public.dam USING btree (name);


--
-- Name: idx_feedback_performance_data_vw_horse_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_feedback_performance_data_vw_horse_id ON public.feedback_performance_data_mat_vw USING btree (horse_id);


--
-- Name: idx_feedback_performance_data_vw_race_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_feedback_performance_data_vw_race_id ON public.feedback_performance_data_mat_vw USING btree (race_id);


--
-- Name: idx_feedback_performance_data_vw_race_time; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_feedback_performance_data_vw_race_time ON public.feedback_performance_data_mat_vw USING btree (race_time);


--
-- Name: idx_historical_price_data_bf_unique_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_historical_price_data_bf_unique_id ON public.historical_price_data USING btree (bf_unique_id);


--
-- Name: idx_historical_price_data_unique_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_historical_price_data_unique_id ON public.historical_price_data USING btree (unique_id);


--
-- Name: idx_horse_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_horse_id ON public.horse USING btree (rp_id);


--
-- Name: idx_horse_id_upd; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_horse_id_upd ON ONLY public.unioned_performance_data USING btree (horse_id);


--
-- Name: idx_horse_name; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_horse_name ON public.horse USING btree (name);


--
-- Name: idx_jockey_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_jockey_id ON public.jockey USING btree (rp_id);


--
-- Name: idx_jockey_name; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_jockey_name ON public.jockey USING btree (name);


--
-- Name: idx_non_uk_ire_performance_data_course_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_course_id ON public.non_uk_ire_performance_data USING btree (course_id);


--
-- Name: idx_non_uk_ire_performance_data_dam_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_dam_id ON public.non_uk_ire_performance_data USING btree (dam_id);


--
-- Name: idx_non_uk_ire_performance_data_horse_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_horse_id ON public.non_uk_ire_performance_data USING btree (horse_id);


--
-- Name: idx_non_uk_ire_performance_data_jockey_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_jockey_id ON public.non_uk_ire_performance_data USING btree (jockey_id);


--
-- Name: idx_non_uk_ire_performance_data_owner_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_owner_id ON public.non_uk_ire_performance_data USING btree (owner_id);


--
-- Name: idx_non_uk_ire_performance_data_race_date; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_race_date ON public.non_uk_ire_performance_data USING btree (race_date);


--
-- Name: idx_non_uk_ire_performance_data_sire_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_sire_id ON public.non_uk_ire_performance_data USING btree (sire_id);


--
-- Name: idx_non_uk_ire_performance_data_trainer_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_trainer_id ON public.non_uk_ire_performance_data USING btree (trainer_id);


--
-- Name: idx_non_uk_ire_performance_data_unique_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_non_uk_ire_performance_data_unique_id ON public.non_uk_ire_performance_data USING btree (unique_id);


--
-- Name: idx_pbf_horse_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_pbf_horse_id ON public.horse USING btree (bf_id);


--
-- Name: idx_performance_data_course_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_course_id ON public.performance_data USING btree (course_id);


--
-- Name: idx_performance_data_dam_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_dam_id ON public.performance_data USING btree (dam_id);


--
-- Name: idx_performance_data_horse_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_horse_id ON public.performance_data USING btree (horse_id);


--
-- Name: idx_performance_data_jockey_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_jockey_id ON public.performance_data USING btree (jockey_id);


--
-- Name: idx_performance_data_owner_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_owner_id ON public.performance_data USING btree (owner_id);


--
-- Name: idx_performance_data_race_date; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_race_date ON public.performance_data USING btree (race_date);


--
-- Name: idx_performance_data_sire_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_sire_id ON public.performance_data USING btree (sire_id);


--
-- Name: idx_performance_data_trainer_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_trainer_id ON public.performance_data USING btree (trainer_id);


--
-- Name: idx_performance_data_unique_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_performance_data_unique_id ON public.performance_data USING btree (unique_id);


--
-- Name: idx_race_data_course_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_race_data_course_id ON public.race_data USING btree (course_id);


--
-- Name: idx_race_data_meeting_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_race_data_meeting_id ON public.race_data USING btree (meeting_id);


--
-- Name: idx_race_data_race_date; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_race_data_race_date ON public.race_data USING btree (race_date);


--
-- Name: idx_race_data_race_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_race_data_race_id ON public.race_data USING btree (race_id);


--
-- Name: idx_race_date_upd; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_race_date_upd ON ONLY public.unioned_performance_data USING btree (race_date);


--
-- Name: idx_race_id_upd; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_race_id_upd ON ONLY public.unioned_performance_data USING btree (race_id);


--
-- Name: idx_sire_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_sire_id ON public.sire USING btree (rp_id);


--
-- Name: idx_sire_name; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_sire_name ON public.sire USING btree (name);


--
-- Name: idx_tf_dam_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_tf_dam_id ON public.dam USING btree (tf_id);


--
-- Name: idx_tf_horse_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_tf_horse_id ON public.horse USING btree (tf_id);


--
-- Name: idx_tf_jockey_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_tf_jockey_id ON public.jockey USING btree (tf_id);


--
-- Name: idx_tf_sire_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_tf_sire_id ON public.sire USING btree (tf_id);


--
-- Name: idx_tf_trainer_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_tf_trainer_id ON public.trainer USING btree (tf_id);


--
-- Name: idx_todays_performance_data_course_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_course_id ON public.todays_performance_data USING btree (course_id);


--
-- Name: idx_todays_performance_data_dam_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_dam_id ON public.todays_performance_data USING btree (dam_id);


--
-- Name: idx_todays_performance_data_horse_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_horse_id ON public.todays_performance_data USING btree (horse_id);


--
-- Name: idx_todays_performance_data_jockey_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_jockey_id ON public.todays_performance_data USING btree (jockey_id);


--
-- Name: idx_todays_performance_data_owner_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_owner_id ON public.todays_performance_data USING btree (owner_id);


--
-- Name: idx_todays_performance_data_race_date; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_race_date ON public.todays_performance_data USING btree (race_date);


--
-- Name: idx_todays_performance_data_sire_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_sire_id ON public.todays_performance_data USING btree (sire_id);


--
-- Name: idx_todays_performance_data_trainer_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_trainer_id ON public.todays_performance_data USING btree (trainer_id);


--
-- Name: idx_todays_performance_data_unique_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_performance_data_unique_id ON public.todays_performance_data USING btree (unique_id);


--
-- Name: idx_todays_performance_data_vw_race_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_todays_performance_data_vw_race_id ON public.todays_performance_data_mat_vw USING btree (race_id);


--
-- Name: idx_todays_performance_data_vw_race_time; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_todays_performance_data_vw_race_time ON public.todays_performance_data_mat_vw USING btree (race_time);


--
-- Name: idx_todays_race_data_course_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_race_data_course_id ON public.todays_race_data USING btree (course_id);


--
-- Name: idx_todays_race_data_meeting_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_race_data_meeting_id ON public.todays_race_data USING btree (meeting_id);


--
-- Name: idx_todays_race_data_race_date; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_race_data_race_date ON public.todays_race_data USING btree (race_date);


--
-- Name: idx_todays_race_data_race_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_todays_race_data_race_id ON public.todays_race_data USING btree (race_id);


--
-- Name: idx_trainer_id; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_trainer_id ON public.trainer USING btree (rp_id);


--
-- Name: idx_trainer_name; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX idx_trainer_name ON public.trainer USING btree (name);


--
-- Name: unioned_performance_data_2010_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2010_horse_id_idx ON public.unioned_performance_data_2010 USING btree (horse_id);


--
-- Name: unioned_performance_data_2010_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2010_race_date_idx ON public.unioned_performance_data_2010 USING btree (race_date);


--
-- Name: unioned_performance_data_2010_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2010_race_id_idx ON public.unioned_performance_data_2010 USING btree (race_id);


--
-- Name: unioned_performance_data_2011_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2011_horse_id_idx ON public.unioned_performance_data_2011 USING btree (horse_id);


--
-- Name: unioned_performance_data_2011_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2011_race_date_idx ON public.unioned_performance_data_2011 USING btree (race_date);


--
-- Name: unioned_performance_data_2011_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2011_race_id_idx ON public.unioned_performance_data_2011 USING btree (race_id);


--
-- Name: unioned_performance_data_2012_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2012_horse_id_idx ON public.unioned_performance_data_2012 USING btree (horse_id);


--
-- Name: unioned_performance_data_2012_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2012_race_date_idx ON public.unioned_performance_data_2012 USING btree (race_date);


--
-- Name: unioned_performance_data_2012_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2012_race_id_idx ON public.unioned_performance_data_2012 USING btree (race_id);


--
-- Name: unioned_performance_data_2013_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2013_horse_id_idx ON public.unioned_performance_data_2013 USING btree (horse_id);


--
-- Name: unioned_performance_data_2013_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2013_race_date_idx ON public.unioned_performance_data_2013 USING btree (race_date);


--
-- Name: unioned_performance_data_2013_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2013_race_id_idx ON public.unioned_performance_data_2013 USING btree (race_id);


--
-- Name: unioned_performance_data_2014_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2014_horse_id_idx ON public.unioned_performance_data_2014 USING btree (horse_id);


--
-- Name: unioned_performance_data_2014_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2014_race_date_idx ON public.unioned_performance_data_2014 USING btree (race_date);


--
-- Name: unioned_performance_data_2014_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2014_race_id_idx ON public.unioned_performance_data_2014 USING btree (race_id);


--
-- Name: unioned_performance_data_2015_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2015_horse_id_idx ON public.unioned_performance_data_2015 USING btree (horse_id);


--
-- Name: unioned_performance_data_2015_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2015_race_date_idx ON public.unioned_performance_data_2015 USING btree (race_date);


--
-- Name: unioned_performance_data_2015_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2015_race_id_idx ON public.unioned_performance_data_2015 USING btree (race_id);


--
-- Name: unioned_performance_data_2016_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2016_horse_id_idx ON public.unioned_performance_data_2016 USING btree (horse_id);


--
-- Name: unioned_performance_data_2016_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2016_race_date_idx ON public.unioned_performance_data_2016 USING btree (race_date);


--
-- Name: unioned_performance_data_2016_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2016_race_id_idx ON public.unioned_performance_data_2016 USING btree (race_id);


--
-- Name: unioned_performance_data_2017_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2017_horse_id_idx ON public.unioned_performance_data_2017 USING btree (horse_id);


--
-- Name: unioned_performance_data_2017_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2017_race_date_idx ON public.unioned_performance_data_2017 USING btree (race_date);


--
-- Name: unioned_performance_data_2017_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2017_race_id_idx ON public.unioned_performance_data_2017 USING btree (race_id);


--
-- Name: unioned_performance_data_2018_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2018_horse_id_idx ON public.unioned_performance_data_2018 USING btree (horse_id);


--
-- Name: unioned_performance_data_2018_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2018_race_date_idx ON public.unioned_performance_data_2018 USING btree (race_date);


--
-- Name: unioned_performance_data_2018_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2018_race_id_idx ON public.unioned_performance_data_2018 USING btree (race_id);


--
-- Name: unioned_performance_data_2019_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2019_horse_id_idx ON public.unioned_performance_data_2019 USING btree (horse_id);


--
-- Name: unioned_performance_data_2019_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2019_race_date_idx ON public.unioned_performance_data_2019 USING btree (race_date);


--
-- Name: unioned_performance_data_2019_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2019_race_id_idx ON public.unioned_performance_data_2019 USING btree (race_id);


--
-- Name: unioned_performance_data_2020_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2020_horse_id_idx ON public.unioned_performance_data_2020 USING btree (horse_id);


--
-- Name: unioned_performance_data_2020_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2020_race_date_idx ON public.unioned_performance_data_2020 USING btree (race_date);


--
-- Name: unioned_performance_data_2020_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2020_race_id_idx ON public.unioned_performance_data_2020 USING btree (race_id);


--
-- Name: unioned_performance_data_2021_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2021_horse_id_idx ON public.unioned_performance_data_2021 USING btree (horse_id);


--
-- Name: unioned_performance_data_2021_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2021_race_date_idx ON public.unioned_performance_data_2021 USING btree (race_date);


--
-- Name: unioned_performance_data_2021_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2021_race_id_idx ON public.unioned_performance_data_2021 USING btree (race_id);


--
-- Name: unioned_performance_data_2022_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2022_horse_id_idx ON public.unioned_performance_data_2022 USING btree (horse_id);


--
-- Name: unioned_performance_data_2022_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2022_race_date_idx ON public.unioned_performance_data_2022 USING btree (race_date);


--
-- Name: unioned_performance_data_2022_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2022_race_id_idx ON public.unioned_performance_data_2022 USING btree (race_id);


--
-- Name: unioned_performance_data_2023_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2023_horse_id_idx ON public.unioned_performance_data_2023 USING btree (horse_id);


--
-- Name: unioned_performance_data_2023_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2023_race_date_idx ON public.unioned_performance_data_2023 USING btree (race_date);


--
-- Name: unioned_performance_data_2023_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2023_race_id_idx ON public.unioned_performance_data_2023 USING btree (race_id);


--
-- Name: unioned_performance_data_2024_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2024_horse_id_idx ON public.unioned_performance_data_2024 USING btree (horse_id);


--
-- Name: unioned_performance_data_2024_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2024_race_date_idx ON public.unioned_performance_data_2024 USING btree (race_date);


--
-- Name: unioned_performance_data_2024_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2024_race_id_idx ON public.unioned_performance_data_2024 USING btree (race_id);


--
-- Name: unioned_performance_data_2025_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2025_horse_id_idx ON public.unioned_performance_data_2025 USING btree (horse_id);


--
-- Name: unioned_performance_data_2025_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2025_race_date_idx ON public.unioned_performance_data_2025 USING btree (race_date);


--
-- Name: unioned_performance_data_2025_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2025_race_id_idx ON public.unioned_performance_data_2025 USING btree (race_id);


--
-- Name: unioned_performance_data_2026_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2026_horse_id_idx ON public.unioned_performance_data_2026 USING btree (horse_id);


--
-- Name: unioned_performance_data_2026_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2026_race_date_idx ON public.unioned_performance_data_2026 USING btree (race_date);


--
-- Name: unioned_performance_data_2026_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2026_race_id_idx ON public.unioned_performance_data_2026 USING btree (race_id);


--
-- Name: unioned_performance_data_2027_horse_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2027_horse_id_idx ON public.unioned_performance_data_2027 USING btree (horse_id);


--
-- Name: unioned_performance_data_2027_race_date_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2027_race_date_idx ON public.unioned_performance_data_2027 USING btree (race_date);


--
-- Name: unioned_performance_data_2027_race_id_idx; Type: INDEX; Schema: public; Owner: tomwattley
--

CREATE INDEX unioned_performance_data_2027_race_id_idx ON public.unioned_performance_data_2027 USING btree (race_id);


--
-- Name: idx_rp_raw_non_uk_performance_data_dam_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_dam_id ON rp_raw.performance_data USING btree (dam_id);


--
-- Name: idx_rp_raw_non_uk_performance_data_horse_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_horse_id ON rp_raw.performance_data USING btree (horse_id);


--
-- Name: idx_rp_raw_non_uk_performance_data_jockey_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_jockey_id ON rp_raw.performance_data USING btree (jockey_id);


--
-- Name: idx_rp_raw_non_uk_performance_data_owner_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_owner_id ON rp_raw.performance_data USING btree (owner_id);


--
-- Name: idx_rp_raw_non_uk_performance_data_race_date; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_race_date ON rp_raw.performance_data USING btree (race_date);


--
-- Name: idx_rp_raw_non_uk_performance_data_race_date_course_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_race_date_course_id ON rp_raw.performance_data USING btree (race_date, course_id);


--
-- Name: idx_rp_raw_non_uk_performance_data_sire_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_sire_id ON rp_raw.performance_data USING btree (sire_id);


--
-- Name: idx_rp_raw_non_uk_performance_data_trainer_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_non_uk_performance_data_trainer_id ON rp_raw.performance_data USING btree (trainer_id);


--
-- Name: idx_rp_raw_performance_data_dam_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_dam_id ON rp_raw.performance_data USING btree (dam_id);


--
-- Name: idx_rp_raw_performance_data_horse_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_horse_id ON rp_raw.performance_data USING btree (horse_id);


--
-- Name: idx_rp_raw_performance_data_jockey_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_jockey_id ON rp_raw.performance_data USING btree (jockey_id);


--
-- Name: idx_rp_raw_performance_data_owner_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_owner_id ON rp_raw.performance_data USING btree (owner_id);


--
-- Name: idx_rp_raw_performance_data_race_date; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_race_date ON rp_raw.performance_data USING btree (race_date);


--
-- Name: idx_rp_raw_performance_data_race_date_course_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_race_date_course_id ON rp_raw.performance_data USING btree (race_date, course_id);


--
-- Name: idx_rp_raw_performance_data_sire_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_sire_id ON rp_raw.performance_data USING btree (sire_id);


--
-- Name: idx_rp_raw_performance_data_trainer_id; Type: INDEX; Schema: rp_raw; Owner: tomwattley
--

CREATE INDEX idx_rp_raw_performance_data_trainer_id ON rp_raw.performance_data USING btree (trainer_id);


--
-- Name: idx_tf_raw_performance_data_dam_id; Type: INDEX; Schema: tf_raw; Owner: tomwattley
--

CREATE INDEX idx_tf_raw_performance_data_dam_id ON tf_raw.performance_data USING btree (dam_id);


--
-- Name: idx_tf_raw_performance_data_horse_id; Type: INDEX; Schema: tf_raw; Owner: tomwattley
--

CREATE INDEX idx_tf_raw_performance_data_horse_id ON tf_raw.performance_data USING btree (horse_id);


--
-- Name: idx_tf_raw_performance_data_jockey_id; Type: INDEX; Schema: tf_raw; Owner: tomwattley
--

CREATE INDEX idx_tf_raw_performance_data_jockey_id ON tf_raw.performance_data USING btree (jockey_id);


--
-- Name: idx_tf_raw_performance_data_race_date; Type: INDEX; Schema: tf_raw; Owner: tomwattley
--

CREATE INDEX idx_tf_raw_performance_data_race_date ON tf_raw.performance_data USING btree (race_date);


--
-- Name: idx_tf_raw_performance_data_race_date_course_id; Type: INDEX; Schema: tf_raw; Owner: tomwattley
--

CREATE INDEX idx_tf_raw_performance_data_race_date_course_id ON tf_raw.performance_data USING btree (race_date, course_id);


--
-- Name: idx_tf_raw_performance_data_sire_id; Type: INDEX; Schema: tf_raw; Owner: tomwattley
--

CREATE INDEX idx_tf_raw_performance_data_sire_id ON tf_raw.performance_data USING btree (sire_id);


--
-- Name: idx_tf_raw_performance_data_trainer_id; Type: INDEX; Schema: tf_raw; Owner: tomwattley
--

CREATE INDEX idx_tf_raw_performance_data_trainer_id ON tf_raw.performance_data USING btree (trainer_id);


--
-- Name: unioned_performance_data_2010_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2010_horse_id_idx;


--
-- Name: unioned_performance_data_2010_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2010_race_date_idx;


--
-- Name: unioned_performance_data_2010_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2010_race_id_idx;


--
-- Name: unioned_performance_data_2010_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2010_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2011_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2011_horse_id_idx;


--
-- Name: unioned_performance_data_2011_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2011_race_date_idx;


--
-- Name: unioned_performance_data_2011_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2011_race_id_idx;


--
-- Name: unioned_performance_data_2011_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2011_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2012_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2012_horse_id_idx;


--
-- Name: unioned_performance_data_2012_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2012_race_date_idx;


--
-- Name: unioned_performance_data_2012_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2012_race_id_idx;


--
-- Name: unioned_performance_data_2012_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2012_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2013_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2013_horse_id_idx;


--
-- Name: unioned_performance_data_2013_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2013_race_date_idx;


--
-- Name: unioned_performance_data_2013_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2013_race_id_idx;


--
-- Name: unioned_performance_data_2013_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2013_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2014_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2014_horse_id_idx;


--
-- Name: unioned_performance_data_2014_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2014_race_date_idx;


--
-- Name: unioned_performance_data_2014_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2014_race_id_idx;


--
-- Name: unioned_performance_data_2014_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2014_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2015_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2015_horse_id_idx;


--
-- Name: unioned_performance_data_2015_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2015_race_date_idx;


--
-- Name: unioned_performance_data_2015_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2015_race_id_idx;


--
-- Name: unioned_performance_data_2015_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2015_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2016_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2016_horse_id_idx;


--
-- Name: unioned_performance_data_2016_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2016_race_date_idx;


--
-- Name: unioned_performance_data_2016_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2016_race_id_idx;


--
-- Name: unioned_performance_data_2016_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2016_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2017_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2017_horse_id_idx;


--
-- Name: unioned_performance_data_2017_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2017_race_date_idx;


--
-- Name: unioned_performance_data_2017_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2017_race_id_idx;


--
-- Name: unioned_performance_data_2017_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2017_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2018_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2018_horse_id_idx;


--
-- Name: unioned_performance_data_2018_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2018_race_date_idx;


--
-- Name: unioned_performance_data_2018_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2018_race_id_idx;


--
-- Name: unioned_performance_data_2018_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2018_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2019_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2019_horse_id_idx;


--
-- Name: unioned_performance_data_2019_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2019_race_date_idx;


--
-- Name: unioned_performance_data_2019_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2019_race_id_idx;


--
-- Name: unioned_performance_data_2019_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2019_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2020_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2020_horse_id_idx;


--
-- Name: unioned_performance_data_2020_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2020_race_date_idx;


--
-- Name: unioned_performance_data_2020_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2020_race_id_idx;


--
-- Name: unioned_performance_data_2020_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2020_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2021_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2021_horse_id_idx;


--
-- Name: unioned_performance_data_2021_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2021_race_date_idx;


--
-- Name: unioned_performance_data_2021_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2021_race_id_idx;


--
-- Name: unioned_performance_data_2021_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2021_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2022_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2022_horse_id_idx;


--
-- Name: unioned_performance_data_2022_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2022_race_date_idx;


--
-- Name: unioned_performance_data_2022_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2022_race_id_idx;


--
-- Name: unioned_performance_data_2022_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2022_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2023_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2023_horse_id_idx;


--
-- Name: unioned_performance_data_2023_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2023_race_date_idx;


--
-- Name: unioned_performance_data_2023_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2023_race_id_idx;


--
-- Name: unioned_performance_data_2023_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2023_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2024_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2024_horse_id_idx;


--
-- Name: unioned_performance_data_2024_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2024_race_date_idx;


--
-- Name: unioned_performance_data_2024_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2024_race_id_idx;


--
-- Name: unioned_performance_data_2024_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2024_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2025_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2025_horse_id_idx;


--
-- Name: unioned_performance_data_2025_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2025_race_date_idx;


--
-- Name: unioned_performance_data_2025_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2025_race_id_idx;


--
-- Name: unioned_performance_data_2025_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2025_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2026_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2026_horse_id_idx;


--
-- Name: unioned_performance_data_2026_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2026_race_date_idx;


--
-- Name: unioned_performance_data_2026_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2026_race_id_idx;


--
-- Name: unioned_performance_data_2026_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2026_unique_id_race_date_key;


--
-- Name: unioned_performance_data_2027_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2027_horse_id_idx;


--
-- Name: unioned_performance_data_2027_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2027_race_date_idx;


--
-- Name: unioned_performance_data_2027_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2027_race_id_idx;


--
-- Name: unioned_performance_data_2027_unique_id_race_date_key; Type: INDEX ATTACH; Schema: public; Owner: tomwattley
--

ALTER INDEX public.unioned_unq_id_cns ATTACH PARTITION public.unioned_performance_data_2027_unique_id_race_date_key;


--
-- PostgreSQL database dump complete
--

