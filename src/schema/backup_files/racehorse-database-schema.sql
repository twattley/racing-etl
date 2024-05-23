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
-- Name: backup; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA backup;


ALTER SCHEMA backup OWNER TO postgres;

--
-- Name: bf_raw; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA bf_raw;


ALTER SCHEMA bf_raw OWNER TO postgres;

--
-- Name: errors; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA errors;


ALTER SCHEMA errors OWNER TO postgres;

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
-- Name: get_day_name(date); Type: FUNCTION; Schema: errors; Owner: postgres
--

CREATE FUNCTION errors.get_day_name(input_date date) RETURNS text
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Returns the name of the day for the given date
    RETURN TO_CHAR(input_date, 'Day');
END;
$$;


ALTER FUNCTION errors.get_day_name(input_date date) OWNER TO postgres;

--
-- Name: refresh_unmatched_rp_dams(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_rp_dams() OWNER TO doadmin;

--
-- Name: refresh_unmatched_rp_horses(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_rp_horses() OWNER TO doadmin;

--
-- Name: refresh_unmatched_rp_jockeys(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_rp_jockeys() OWNER TO doadmin;

--
-- Name: refresh_unmatched_rp_sires(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_rp_sires() OWNER TO doadmin;

--
-- Name: refresh_unmatched_rp_trainers(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_rp_trainers() OWNER TO doadmin;

--
-- Name: refresh_unmatched_tf_dams(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_tf_dams() OWNER TO doadmin;

--
-- Name: refresh_unmatched_tf_horses(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_tf_horses() OWNER TO doadmin;

--
-- Name: refresh_unmatched_tf_jockeys(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_tf_jockeys() OWNER TO doadmin;

--
-- Name: refresh_unmatched_tf_sires(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_tf_sires() OWNER TO doadmin;

--
-- Name: refresh_unmatched_tf_trainers(); Type: PROCEDURE; Schema: metrics; Owner: doadmin
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


ALTER PROCEDURE metrics.refresh_unmatched_tf_trainers() OWNER TO doadmin;

--
-- Name: insert_todays_transformed_performance_data(); Type: PROCEDURE; Schema: public; Owner: doadmin
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


ALTER PROCEDURE public.insert_todays_transformed_performance_data() OWNER TO doadmin;

--
-- Name: insert_todays_transformed_race_data(); Type: PROCEDURE; Schema: public; Owner: doadmin
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


ALTER PROCEDURE public.insert_todays_transformed_race_data() OWNER TO doadmin;

--
-- Name: insert_transformed_performance_data(); Type: PROCEDURE; Schema: public; Owner: doadmin
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


ALTER PROCEDURE public.insert_transformed_performance_data() OWNER TO doadmin;

--
-- Name: insert_transformed_race_data(); Type: PROCEDURE; Schema: public; Owner: doadmin
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


ALTER PROCEDURE public.insert_transformed_race_data() OWNER TO doadmin;

--
-- Name: insert_unioned_performance_data(); Type: PROCEDURE; Schema: public; Owner: doadmin
--

CREATE PROCEDURE public.insert_unioned_performance_data()
    LANGUAGE plpgsql
    AS $$
BEGIN

	TRUNCATE public.unioned_performance_data;
	
    INSERT INTO public.unioned_performance_data
    (
    horse_name,
    age,
    horse_sex,
	days_since_last_ran,
    draw,
    headgear,
    weight_carried,
    weight_carried_lbs,
    extra_weight,
    jockey_claim,
    finishing_position,
    total_distance_beaten,
    number_of_runs,
    first_places,
    second_places,
    third_places,
    fourth_places,
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
    jockey
    )
with todays as (
	SELECT 
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
				tpd.official_rating,
				tpd.ts,
				tpd.rpr,
				tpd.tfr,
				tpd.tfig,
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
                trd.surface,
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
				j.name as jockey
	FROM todays_performance_data tpd
	LEFT JOIN todays_race_data trd ON tpd.race_id = trd.race_id
	LEFT JOIN course c ON tpd.course_id = c.id
	LEFT JOIN horse h ON tpd.horse_id = h.id
	LEFT JOIN dam d ON tpd.dam_id = d.id
	LEFT JOIN sire s ON tpd.sire_id = s.id
	LEFT JOIN trainer t ON tpd.trainer_id = t.id
	LEFT JOIN jockey j ON tpd.jockey_id = j.id),
historical as (SELECT
			    hpd.horse_name,
				hpd.age,
				hpd.horse_sex,
				hpd.draw,
				hpd.headgear,
				hpd.weight_carried,
				hpd.weight_carried_lbs,
				hpd.extra_weight,
				hpd.jockey_claim,
				hpd.finishing_position,
				hpd.total_distance_beaten,
				hpd.industry_sp,
				hpd.betfair_win_sp,
				hpd.betfair_place_sp,
				hpd.official_rating,
				hpd.ts,
				hpd.rpr,
				hpd.tfr,
				hpd.tfig,
				hpd.in_play_high,
				hpd.in_play_low,
				hpd.in_race_comment,
				hpd.tf_comment,
				hpd.tfr_view,
                hrd.race_id,
				hpd.horse_id,
				hpd.jockey_id,
				hpd.trainer_id,
				hpd.owner_id,
				hpd.sire_id,
				hpd.dam_id,
				hpd.unique_id,
                hrd.race_time,
                hrd.race_date,
                hrd.race_title,
                hrd.race_type,
                hrd.race_class,
                hrd.distance,
                hrd.distance_yards,
                hrd.distance_meters,
                hrd.distance_kilometers,
                hrd.conditions,
                hrd.going,
                hrd.number_of_runners,
                hrd.hcap_range,
                hrd.age_range,
                hrd.surface,
                hrd.total_prize_money,
                hrd.first_place_prize_money,
                hrd.winning_time,
                hrd.time_seconds,
                hrd.relative_time,
                hrd.relative_to_standard,
                hrd.country,
                hrd.main_race_comment,
                hrd.meeting_id,
                hrd.course_id,
				c.name as course, 
				d.name as dam, 
				s.name as sire, 
				t.name as trainer, 
				j.name as jockey
	FROM performance_data hpd
	LEFT JOIN race_data hrd ON hpd.race_id = hrd.race_id
	LEFT JOIN course c ON hpd.course_id = c.id
	LEFT JOIN horse h ON hpd.horse_id = h.id
	LEFT JOIN dam d ON hpd.dam_id = d.id
	LEFT JOIN sire s ON hpd.sire_id = s.id
	LEFT JOIN trainer t ON hpd.trainer_id = t.id
	LEFT JOIN jockey j ON hpd.jockey_id = j.id
)
SELECT 
    horse_name,
    age,
    horse_sex,
	days_since_last_ran,
    draw,
    headgear,
    weight_carried,
    weight_carried_lbs,
    extra_weight,
    jockey_claim,
    finishing_position,
    total_distance_beaten,
    number_of_runs,
    first_places,
    second_places,
    third_places,
    fourth_places,
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
    jockey
FROM (
    SELECT *,
           CASE
               WHEN LAG(race_date) OVER (PARTITION BY horse_id ORDER BY race_date) IS NOT NULL THEN race_date - LAG(race_date) OVER (PARTITION BY horse_id ORDER BY race_date)
               ELSE NULL
           END as days_since_last_ran,
           COUNT(*) OVER (PARTITION BY horse_id ORDER BY race_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as number_of_runs,
           SUM(CASE WHEN finishing_position = '1' THEN 1 ELSE 0 END) OVER (PARTITION BY horse_id ORDER BY race_time) as first_places,
           SUM(CASE WHEN finishing_position = '2' THEN 1 ELSE 0 END) OVER (PARTITION BY horse_id ORDER BY race_time) as second_places,
           SUM(CASE WHEN finishing_position = '3' AND number_of_runners > 7 THEN 1 ELSE 0 END) OVER (PARTITION BY horse_id ORDER BY race_time) as third_places,
           SUM(CASE WHEN finishing_position = '4' AND number_of_runners > 11 THEN 1 ELSE 0 END) OVER (PARTITION BY horse_id ORDER BY race_time) as fourth_places
    FROM (
        SELECT * FROM todays
        UNION 
        SELECT * FROM historical
    ) combined_data
);
END;
$$;


ALTER PROCEDURE public.insert_unioned_performance_data() OWNER TO doadmin;

--
-- Name: minus_three_years(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.minus_three_years(input_date date) RETURNS TABLE(horse_name character varying, age integer, horse_sex character varying, draw integer, headgear character varying, weight_carried character varying, weight_carried_lbs smallint, extra_weight smallint, jockey_claim smallint, finishing_position character varying, total_distance_beaten numeric, industry_sp character varying, betfair_win_sp numeric, betfair_place_sp numeric, official_rating smallint, ts smallint, rpr smallint, tfr smallint, tfig smallint, in_play_high numeric, in_play_low numeric, in_race_comment text, tf_comment text, tfr_view character varying, race_id integer, horse_id integer, jockey_id integer, trainer_id integer, owner_id integer, sire_id integer, dam_id integer, unique_id character varying, race_time timestamp without time zone, race_date date, race_title character varying, race_type character varying, race_class smallint, distance character varying, distance_yards numeric, distance_meters numeric, distance_kilometers numeric, conditions character varying, going character varying, number_of_runners smallint, hcap_range character varying, age_range character varying, surface character varying, total_prize_money integer, first_place_prize_money integer, winning_time character varying, time_seconds numeric, relative_time numeric, relative_to_standard character varying, country character varying, main_race_comment text, meeting_id character varying, course_id smallint, course character varying, dam character varying, sire character varying, trainer character varying, jockey character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 
SELECT *
FROM public.unioned_performance_data ud
WHERE ud.race_date BETWEEN '2020-01-01'::date AND '2023-01-01'::date;

END;
$$;


ALTER FUNCTION public.minus_three_years(input_date date) OWNER TO postgres;

--
-- Name: select_collateral_form_data(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.select_collateral_form_data(input_date date) RETURNS TABLE(horse_name character varying, age integer, horse_sex character varying, draw integer, headgear character varying, weight_carried character varying, weight_carried_lbs smallint, extra_weight smallint, jockey_claim smallint, finishing_position character varying, total_distance_beaten numeric, industry_sp character varying, betfair_win_sp numeric, betfair_place_sp numeric, official_rating smallint, ts smallint, rpr smallint, tfr smallint, tfig smallint, in_play_high numeric, in_play_low numeric, in_race_comment text, tf_comment text, tfr_view character varying, race_id integer, horse_id integer, jockey_id integer, trainer_id integer, owner_id integer, sire_id integer, dam_id integer, unique_id character varying, race_time timestamp without time zone, race_date date, race_title character varying, race_type character varying, race_class smallint, distance character varying, distance_yards numeric, distance_meters numeric, distance_kilometers numeric, conditions character varying, going character varying, number_of_runners smallint, hcap_range character varying, age_range character varying, surface character varying, total_prize_money integer, first_place_prize_money integer, winning_time character varying, time_seconds numeric, relative_time numeric, relative_to_standard character varying, country character varying, main_race_comment text, meeting_id character varying, course_id smallint, course character varying, dam character varying, sire character varying, trainer character varying, jockey character varying, data_type character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 

-- Collateral races for horses that have raced with horses from historical races
SELECT pd.*, 'collateral'::character varying AS data_type
FROM public.unioned_performance_data pd
WHERE pd.horse_id IN (
    SELECT DISTINCT pd.horse_id
    FROM public.unioned_performance_data pd
    WHERE pd.race_date < input_date
    AND pd.race_date >= input_date - INTERVAL '3 YEARS'
    AND pd.race_id IN (
        SELECT DISTINCT pd.race_id
        FROM public.unioned_performance_data pd
        WHERE pd.horse_id IN (
            SELECT pd.horse_id
            FROM public.unioned_performance_data pd
            WHERE pd.race_date = input_date
        )
        AND pd.race_date < input_date
        AND pd.race_date >= input_date - INTERVAL '3 YEARS'
    )
)
AND pd.race_date < input_date
AND pd.race_date >= input_date - INTERVAL '3 YEARS';

END;
$$;


ALTER FUNCTION public.select_collateral_form_data(input_date date) OWNER TO postgres;

--
-- Name: select_form_data(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.select_form_data(input_date date) RETURNS TABLE(horse_name character varying, age integer, horse_sex character varying, draw integer, headgear character varying, weight_carried character varying, weight_carried_lbs smallint, extra_weight smallint, jockey_claim smallint, finishing_position character varying, total_distance_beaten numeric, industry_sp character varying, betfair_win_sp numeric, betfair_place_sp numeric, official_rating smallint, ts smallint, rpr smallint, tfr smallint, tfig smallint, in_play_high numeric, in_play_low numeric, in_race_comment text, tf_comment text, tfr_view character varying, race_id integer, horse_id integer, jockey_id integer, trainer_id integer, owner_id integer, sire_id integer, dam_id integer, unique_id character varying, race_time timestamp without time zone, race_date date, race_title character varying, race_type character varying, race_class smallint, distance character varying, distance_yards numeric, distance_meters numeric, distance_kilometers numeric, conditions character varying, going character varying, number_of_runners smallint, hcap_range character varying, age_range character varying, surface character varying, total_prize_money integer, first_place_prize_money integer, winning_time character varying, time_seconds numeric, relative_time numeric, relative_to_standard character varying, country character varying, main_race_comment text, meeting_id character varying, course_id smallint, course character varying, dam character varying, sire character varying, trainer character varying, jockey character varying, data_type character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 
-- Historical races for horses racing today
SELECT pd.*, 'historical'::character varying AS data_type
FROM public.unioned_performance_data pd
WHERE pd.horse_id IN (
    SELECT pd.horse_id
    FROM public.unioned_performance_data pd
    WHERE pd.race_date = input_date
)
AND pd.race_date < input_date
AND pd.race_date >= input_date - INTERVAL '3 YEARS';



END;
$$;


ALTER FUNCTION public.select_form_data(input_date date) OWNER TO postgres;

--
-- Name: select_form_data_by_race_id(date, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.select_form_data_by_race_id(input_date date, input_race_id integer) RETURNS TABLE(horse_name character varying, age integer, horse_sex character varying, days_since_last_ran smallint, draw integer, headgear character varying, weight_carried character varying, weight_carried_lbs smallint, extra_weight smallint, jockey_claim smallint, finishing_position character varying, total_distance_beaten numeric, number_of_runs smallint, first_places smallint, second_places smallint, third_places smallint, fourth_places smallint, industry_sp character varying, betfair_win_sp numeric, betfair_place_sp numeric, official_rating smallint, ts smallint, rpr smallint, tfr smallint, tfig smallint, in_play_high numeric, in_play_low numeric, in_race_comment text, tf_comment text, tfr_view character varying, race_id integer, horse_id integer, jockey_id integer, trainer_id integer, owner_id integer, sire_id integer, dam_id integer, unique_id character varying, race_time timestamp without time zone, race_date date, race_title character varying, race_type character varying, race_class smallint, distance character varying, distance_yards numeric, distance_meters numeric, distance_kilometers numeric, conditions character varying, going character varying, number_of_runners smallint, hcap_range character varying, age_range character varying, surface character varying, total_prize_money integer, first_place_prize_money integer, winning_time character varying, time_seconds numeric, relative_time numeric, relative_to_standard character varying, country character varying, main_race_comment text, meeting_id character varying, course_id smallint, course character varying, dam character varying, sire character varying, trainer character varying, jockey character varying, data_type character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 
SELECT pd.*, 'historical'::character varying AS data_type
FROM public.unioned_performance_data pd
WHERE pd.horse_id IN (
    SELECT pd.horse_id
    FROM public.unioned_performance_data pd
    WHERE pd.race_date = input_date
	AND pd.race_id = input_race_id
)
AND pd.race_date < input_date
AND pd.race_date >= input_date - INTERVAL '3 YEARS'
ORDER BY pd.horse_name, pd.race_time DESC;
END;
$$;


ALTER FUNCTION public.select_form_data_by_race_id(input_date date, input_race_id integer) OWNER TO postgres;

--
-- Name: select_race_date(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.select_race_date(input_date date) RETURNS TABLE(horse_name character varying, age integer, horse_sex character varying, draw integer, headgear character varying, weight_carried character varying, weight_carried_lbs smallint, extra_weight smallint, jockey_claim smallint, finishing_position character varying, total_distance_beaten numeric, industry_sp character varying, betfair_win_sp numeric, betfair_place_sp numeric, official_rating smallint, ts smallint, rpr smallint, tfr smallint, tfig smallint, in_play_high numeric, in_play_low numeric, in_race_comment text, tf_comment text, tfr_view character varying, race_id integer, horse_id integer, jockey_id integer, trainer_id integer, owner_id integer, sire_id integer, dam_id integer, unique_id character varying, race_time timestamp without time zone, race_date date, race_title character varying, race_type character varying, race_class smallint, distance character varying, distance_yards numeric, distance_meters numeric, distance_kilometers numeric, conditions character varying, going character varying, number_of_runners smallint, hcap_range character varying, age_range character varying, surface character varying, total_prize_money integer, first_place_prize_money integer, winning_time character varying, time_seconds numeric, relative_time numeric, relative_to_standard character varying, country character varying, main_race_comment text, meeting_id character varying, course_id smallint, course character varying, dam character varying, sire character varying, trainer character varying, jockey character varying, data_type character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 
-- Today's races
SELECT pd.*, 'today'::character varying AS data_type
FROM public.unioned_performance_data pd
WHERE pd.race_date = input_date

UNION ALL

-- Historical races for horses racing today
SELECT pd.*, 'historical'::character varying AS data_type
FROM public.unioned_performance_data pd
WHERE pd.horse_id IN (
    SELECT pd.horse_id
    FROM public.unioned_performance_data pd
    WHERE pd.race_date = input_date
)
AND pd.race_date < input_date
AND pd.race_date >= input_date - INTERVAL '3 YEARS'

UNION ALL

-- Collateral races for horses that have raced with horses from historical races
SELECT pd.*, 'collateral'::character varying AS data_type
FROM public.unioned_performance_data pd
WHERE pd.horse_id IN (
    SELECT DISTINCT pd.horse_id
    FROM public.unioned_performance_data pd
    WHERE pd.race_date < input_date
    AND pd.race_date >= input_date - INTERVAL '3 YEARS'
    AND pd.race_id IN (
        SELECT DISTINCT pd.race_id
        FROM public.unioned_performance_data pd
        WHERE pd.horse_id IN (
            SELECT pd.horse_id
            FROM public.unioned_performance_data pd
            WHERE pd.race_date = input_date
        )
        AND pd.race_date < input_date
        AND pd.race_date >= input_date - INTERVAL '3 YEARS'
    )
)
AND pd.race_date < input_date
AND pd.race_date >= input_date - INTERVAL '3 YEARS';

END;
$$;


ALTER FUNCTION public.select_race_date(input_date date) OWNER TO postgres;

--
-- Name: select_race_date_race_times(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.select_race_date_race_times(input_date date) RETURNS TABLE(race_id integer, race_time timestamp without time zone, race_date date, race_title character varying, race_type character varying, race_class smallint, distance character varying, distance_yards numeric, distance_meters numeric, distance_kilometers numeric, conditions character varying, going character varying, number_of_runners smallint, hcap_range character varying, age_range character varying, surface character varying, total_prize_money integer, first_place_prize_money integer, course_id smallint, course character varying, data_type character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 
-- Today's races
with distinct_races as (
SELECT distinct on (pd.race_id)
pd.race_id,
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
pd.course_id,
pd.course, 
'today'::character varying AS data_type
FROM public.unioned_performance_data pd
WHERE pd.race_date = input_date)

select * from distinct_races
order by course, race_time;
END;
$$;


ALTER FUNCTION public.select_race_date_race_times(input_date date) OWNER TO postgres;

--
-- Name: select_results_data_by_race_id(date, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.select_results_data_by_race_id(input_date date, input_race_id integer) RETURNS TABLE(horse_name character varying, finishing_position character varying, total_distance_beaten numeric, industry_sp character varying, betfair_win_sp numeric, betfair_place_sp numeric, ts smallint, rpr smallint, tfr smallint, tfig smallint, in_play_high numeric, in_play_low numeric, in_race_comment text, tf_comment text, tfr_view character varying, race_id integer, horse_id integer, unique_id character varying, winning_time character varying, time_seconds numeric, relative_time numeric, relative_to_standard character varying, main_race_comment text, meeting_id character varying)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY 

SELECT pd.horse_name,
pd.finishing_position,
pd.total_distance_beaten,
pd.industry_sp,
pd.betfair_win_sp,
pd.betfair_place_sp,
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
pd.unique_id,
pd.winning_time,
pd.time_seconds,
pd.relative_time,
pd.relative_to_standard,
pd.main_race_comment,
pd.meeting_id
FROM public.unioned_performance_data pd
WHERE pd.horse_id IN (
    SELECT pd.horse_id
    FROM public.unioned_performance_data pd
    WHERE pd.race_date = input_date
	AND pd.race_id = input_race_id
)
AND pd.race_date = input_date
ORDER BY pd.total_distance_beaten;
END;
$$;


ALTER FUNCTION public.select_results_data_by_race_id(input_date date, input_race_id integer) OWNER TO postgres;

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
	FROM joined_data WHERE rn = 1;
END;
$$;


ALTER PROCEDURE staging.insert_into_joined_performance_data() OWNER TO doadmin;

--
-- Name: insert_into_todays_joined_performance_data(); Type: PROCEDURE; Schema: staging; Owner: doadmin
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


ALTER PROCEDURE staging.insert_into_todays_joined_performance_data() OWNER TO doadmin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: rp_raw_performance_data; Type: TABLE; Schema: backup; Owner: postgres
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


ALTER TABLE backup.rp_raw_performance_data OWNER TO postgres;

--
-- Name: rp_raw_performance_data_deduped; Type: TABLE; Schema: backup; Owner: postgres
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


ALTER TABLE backup.rp_raw_performance_data_deduped OWNER TO postgres;

--
-- Name: tf_raw_performance_data; Type: TABLE; Schema: backup; Owner: postgres
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


ALTER TABLE backup.tf_raw_performance_data OWNER TO postgres;

--
-- Name: tf_raw_performance_data_deduped; Type: TABLE; Schema: backup; Owner: postgres
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


ALTER TABLE backup.tf_raw_performance_data_deduped OWNER TO postgres;

--
-- Name: todays_price_data; Type: TABLE; Schema: bf_raw; Owner: doadmin
--

CREATE TABLE bf_raw.todays_price_data (
    race_time timestamp without time zone,
    horse_id integer,
    horse_name character varying(132),
    course character varying(132),
    betfair_win_sp numeric(6,2),
    betfair_place_sp numeric(6,2),
    created_at timestamp with time zone,
    status character varying(64)
);


ALTER TABLE bf_raw.todays_price_data OWNER TO doadmin;

--
-- Name: todays_performance_data; Type: TABLE; Schema: rp_raw; Owner: doadmin
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


ALTER TABLE rp_raw.todays_performance_data OWNER TO doadmin;

--
-- Name: todays_performance_data; Type: TABLE; Schema: tf_raw; Owner: doadmin
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


ALTER TABLE tf_raw.todays_performance_data OWNER TO doadmin;

--
-- Name: missing_todays_races; Type: VIEW; Schema: errors; Owner: postgres
--

CREATE VIEW errors.missing_todays_races AS
 WITH rp_data AS (
         SELECT DISTINCT todays_performance_data.race_date,
            todays_performance_data.course,
            todays_performance_data.race_timestamp
           FROM rp_raw.todays_performance_data
          WHERE ((todays_performance_data.race_date)::date >= CURRENT_DATE)
          ORDER BY todays_performance_data.race_date, todays_performance_data.course, todays_performance_data.race_timestamp
        ), tf_data AS (
         SELECT DISTINCT todays_performance_data.race_date,
            todays_performance_data.course,
            todays_performance_data.race_timestamp
           FROM tf_raw.todays_performance_data
          WHERE ((todays_performance_data.race_date)::date >= CURRENT_DATE)
        ), ordered AS (
         SELECT rp.race_date,
            rp.course,
            rp.race_timestamp,
                CASE
                    WHEN ((rp.race_timestamp IS NOT NULL) AND (tf.race_timestamp IS NOT NULL)) THEN true
                    ELSE false
                END AS both_sets
           FROM (rp_data rp
             LEFT JOIN tf_data tf ON ((rp.race_timestamp = tf.race_timestamp)))
          ORDER BY rp.race_date, rp.course, rp.race_timestamp
        )
 SELECT race_date,
    course,
    race_timestamp,
    both_sets
   FROM ordered;


ALTER VIEW errors.missing_todays_races OWNER TO postgres;

--
-- Name: staging_entity_unmatched; Type: TABLE; Schema: errors; Owner: doadmin
--

CREATE TABLE errors.staging_entity_unmatched (
    entity text,
    race_timestamp timestamp without time zone,
    name text,
    debug_link text
);


ALTER TABLE errors.staging_entity_unmatched OWNER TO doadmin;

--
-- Name: staging_todays_transformed_performance_data_rejected; Type: TABLE; Schema: errors; Owner: tom.wattley
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


ALTER TABLE errors.staging_todays_transformed_performance_data_rejected OWNER TO "tom.wattley";

--
-- Name: staging_transformed_performance_data_rejected; Type: TABLE; Schema: errors; Owner: postgres
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


ALTER TABLE errors.staging_transformed_performance_data_rejected OWNER TO postgres;

--
-- Name: todays_performance_data; Type: TABLE; Schema: errors; Owner: postgres
--

CREATE TABLE errors.todays_performance_data (
    error_url text,
    error_message text,
    error_processing_time timestamp without time zone
);


ALTER TABLE errors.todays_performance_data OWNER TO postgres;

--
-- Name: performance_data; Type: TABLE; Schema: public; Owner: doadmin
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


ALTER TABLE public.performance_data OWNER TO doadmin;

--
-- Name: performance_data; Type: TABLE; Schema: rp_raw; Owner: doadmin
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


ALTER TABLE rp_raw.performance_data OWNER TO doadmin;

--
-- Name: missing_raw_data; Type: VIEW; Schema: metrics; Owner: postgres
--

CREATE VIEW metrics.missing_raw_data AS
 SELECT rpd.race_timestamp,
    rpd.horse_name
   FROM (rp_raw.performance_data rpd
     LEFT JOIN public.performance_data pd ON (((rpd.unique_id)::text = (pd.unique_id)::text)))
  WHERE (pd.unique_id IS NULL);


ALTER VIEW metrics.missing_raw_data OWNER TO postgres;

--
-- Name: processing_times; Type: TABLE; Schema: metrics; Owner: postgres
--

CREATE TABLE metrics.processing_times (
    job_name character varying(132),
    processed_at timestamp without time zone
);


ALTER TABLE metrics.processing_times OWNER TO postgres;

--
-- Name: course; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.course (
    name character varying(132),
    id smallint NOT NULL,
    rp_name character varying(132),
    rp_id character varying(4),
    tf_name character varying(132),
    tf_id character varying(4),
    country_id character varying(4),
    error_id smallint
);


ALTER TABLE public.course OWNER TO doadmin;

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
    unique_id character varying(132)
);


ALTER TABLE tf_raw.performance_data OWNER TO doadmin;

--
-- Name: record_count_differences_vw; Type: VIEW; Schema: metrics; Owner: doadmin
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


ALTER VIEW metrics.record_count_differences_vw OWNER TO doadmin;

--
-- Name: bf_horse; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bf_horse (
    id integer,
    name character varying(132),
    bf_id integer
);


ALTER TABLE public.bf_horse OWNER TO postgres;

--
-- Name: country; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.country (
    country_id integer,
    country_name character varying(132)
);


ALTER TABLE public.country OWNER TO doadmin;

--
-- Name: dam; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.dam (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.dam OWNER TO doadmin;

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
-- Name: horse; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.horse (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.horse OWNER TO doadmin;

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
-- Name: jockey; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.jockey (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.jockey OWNER TO doadmin;

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
-- Name: joined_performance_data; Type: TABLE; Schema: staging; Owner: doadmin
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


ALTER TABLE staging.joined_performance_data OWNER TO doadmin;

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


ALTER VIEW public.missing_performance_data_vw OWNER TO doadmin;

--
-- Name: race_data; Type: TABLE; Schema: public; Owner: doadmin
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


ALTER TABLE public.race_data OWNER TO doadmin;

--
-- Name: missing_race_data_vw; Type: VIEW; Schema: public; Owner: doadmin
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


ALTER VIEW public.missing_race_data_vw OWNER TO doadmin;

--
-- Name: todays_joined_performance_data; Type: TABLE; Schema: staging; Owner: doadmin
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


ALTER TABLE staging.todays_joined_performance_data OWNER TO doadmin;

--
-- Name: missing_todays_performance_data_vw; Type: VIEW; Schema: public; Owner: doadmin
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


ALTER VIEW public.missing_todays_performance_data_vw OWNER TO doadmin;

--
-- Name: missing_todays_race_data_vw; Type: VIEW; Schema: public; Owner: doadmin
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


ALTER VIEW public.missing_todays_race_data_vw OWNER TO doadmin;

--
-- Name: owner; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.owner (
    rp_id character varying(32),
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
-- Name: sire; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.sire (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.sire OWNER TO doadmin;

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
-- Name: todays_performance_data; Type: TABLE; Schema: public; Owner: doadmin
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


ALTER TABLE public.todays_performance_data OWNER TO doadmin;

--
-- Name: todays_race_data; Type: TABLE; Schema: public; Owner: doadmin
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


ALTER TABLE public.todays_race_data OWNER TO doadmin;

--
-- Name: trainer; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.trainer (
    rp_id character varying(32),
    name character varying(132),
    tf_id character varying(32),
    id integer NOT NULL
);


ALTER TABLE public.trainer OWNER TO doadmin;

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
-- Name: unioned_performance_data; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.unioned_performance_data (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
)
PARTITION BY RANGE (race_date);


ALTER TABLE public.unioned_performance_data OWNER TO doadmin;

--
-- Name: unioned_performance_data_2010; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2010 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2010 OWNER TO postgres;

--
-- Name: unioned_performance_data_2011; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2011 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2011 OWNER TO postgres;

--
-- Name: unioned_performance_data_2012; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2012 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2012 OWNER TO postgres;

--
-- Name: unioned_performance_data_2013; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2013 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2013 OWNER TO postgres;

--
-- Name: unioned_performance_data_2014; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2014 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2014 OWNER TO postgres;

--
-- Name: unioned_performance_data_2015; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2015 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2015 OWNER TO postgres;

--
-- Name: unioned_performance_data_2016; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2016 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2016 OWNER TO postgres;

--
-- Name: unioned_performance_data_2017; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2017 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2017 OWNER TO postgres;

--
-- Name: unioned_performance_data_2018; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2018 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2018 OWNER TO postgres;

--
-- Name: unioned_performance_data_2019; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2019 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2019 OWNER TO postgres;

--
-- Name: unioned_performance_data_2020; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2020 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2020 OWNER TO postgres;

--
-- Name: unioned_performance_data_2021; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2021 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2021 OWNER TO postgres;

--
-- Name: unioned_performance_data_2022; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2022 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2022 OWNER TO postgres;

--
-- Name: unioned_performance_data_2023; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2023 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2023 OWNER TO postgres;

--
-- Name: unioned_performance_data_2024; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2024 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2024 OWNER TO postgres;

--
-- Name: unioned_performance_data_2025; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2025 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2025 OWNER TO postgres;

--
-- Name: unioned_performance_data_2026; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2026 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2026 OWNER TO postgres;

--
-- Name: unioned_performance_data_2027; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.unioned_performance_data_2027 (
    horse_name character varying(132),
    age integer,
    horse_sex character varying(32),
    days_since_last_ran smallint,
    draw integer,
    headgear character varying(64),
    weight_carried character varying(16),
    weight_carried_lbs smallint,
    extra_weight smallint,
    jockey_claim smallint,
    finishing_position character varying(6),
    total_distance_beaten numeric(10,2),
    number_of_runs smallint,
    first_places smallint,
    second_places smallint,
    third_places smallint,
    fourth_places smallint,
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
    jockey character varying(132)
);


ALTER TABLE public.unioned_performance_data_2027 OWNER TO postgres;

--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    username character varying(255) NOT NULL,
    hashed_password character varying(255) NOT NULL,
    email character varying(255) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean DEFAULT true,
    is_admin boolean DEFAULT false
);


ALTER TABLE public.users OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.users_id_seq OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: best_horses; Type: VIEW; Schema: rp_raw; Owner: postgres
--

CREATE VIEW rp_raw.best_horses AS
 SELECT DISTINCT pd.horse_name,
    pd.horse_id
   FROM (public.performance_data pd
     LEFT JOIN public.race_data rd ON ((pd.race_id = rd.race_id)))
  WHERE ((rd.race_class = 1) AND ((rd.race_type)::text = 'Flat'::text) AND (rd.race_time > '2010-01-01 00:00:00'::timestamp without time zone));


ALTER VIEW rp_raw.best_horses OWNER TO postgres;

--
-- Name: days_results_links; Type: TABLE; Schema: rp_raw; Owner: doadmin
--

CREATE TABLE rp_raw.days_results_links (
    date date,
    link text
);


ALTER TABLE rp_raw.days_results_links OWNER TO doadmin;

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
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2010-01-01'::date) AND (cr.course_part = ANY (ARRAY['aintree'::text, 'ascot'::text, 'ayr'::text, 'ballinrobe'::text, 'bangor-on-dee'::text, 'bath'::text, 'bellewstown'::text, 'beverley'::text, 'brighton'::text, 'carlisle'::text, 'cartmel'::text, 'catterick'::text, 'chelmsford-aw'::text, 'cheltenham'::text, 'chepstow'::text, 'chester'::text, 'clonmel'::text, 'cork'::text, 'curragh'::text, 'doncaster'::text, 'down-royal'::text, 'downpatrick'::text, 'dundalk-aw'::text, 'epsom'::text, 'exeter'::text, 'fairyhouse'::text, 'fakenham'::text, 'ffos-las'::text, 'fontwell'::text, 'galway'::text, 'goodwood'::text, 'gowran-park'::text, 'hamilton'::text, 'haydock'::text, 'hereford'::text, 'hexham'::text, 'huntingdon'::text, 'kelso'::text, 'kempton'::text, 'kempton-aw'::text, 'kilbeggan'::text, 'killarney'::text, 'laytown'::text, 'leicester'::text, 'leopardstown'::text, 'limerick'::text, 'lingfield'::text, 'lingfield-aw'::text, 'listowel'::text, 'ludlow'::text, 'market-rasen'::text, 'musselburgh'::text, 'naas'::text, 'navan'::text, 'newbury'::text, 'newcastle'::text, 'newcastle-aw'::text, 'newmarket'::text, 'newmarket-july'::text, 'newton-abbot'::text, 'nottingham'::text, 'perth'::text, 'plumpton'::text, 'pontefract'::text, 'punchestown'::text, 'redcar'::text, 'ripon'::text, 'roscommon'::text, 'salisbury'::text, 'sandown'::text, 'sedgefield'::text, 'sligo'::text, 'southwell'::text, 'southwell-aw'::text, 'stratford'::text, 'taunton'::text, 'thirsk'::text, 'thurles'::text, 'tipperary'::text, 'towcester'::text, 'tramore'::text, 'uttoxeter'::text, 'warwick'::text, 'wetherby'::text, 'wexford'::text, 'wexford-rh'::text, 'wincanton'::text, 'windsor'::text, 'wolverhampton-aw'::text, 'worcester'::text, 'yarmouth'::text, 'york'::text])) AND (cr.link <> ALL (ARRAY['https://www.racingpost.com/results/87/wetherby/2011-10-12/539792'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2007-12-10/444939'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/101/worcester/2023-05-26/839708'::text, 'https://www.racingpost.com/results/15/doncaster/2009-12-12/494914'::text, 'https://www.racingpost.com/results/83/towcester/2011-03-17/525075'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2022-03-04/804298'::text, 'https://www.racingpost.com/results/25/hexham/2021-05-01/781541'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2010-10-03/515581'::text, 'https://www.racingpost.com/results/17/epsom/2014-08-25/608198'::text, 'https://www.racingpost.com/results/57/sedgefield/2014-11-25/613650'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2017-02-23/668139'::text, 'https://www.racingpost.com/results/41/perth/2017-09-11/682957'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2020-03-20/754104'::text, 'https://www.racingpost.com/results/20/fontwell/2016-09-30/658390'::text, 'https://www.racingpost.com/results/9/cartmel/2018-05-30/701255'::text, 'https://www.racingpost.com/results/34/ludlow/2014-05-11/600195'::text, 'https://www.racingpost.com/results/8/carlisle/2011-02-21/523611'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/90/wincanton/2016-12-26/664813'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/44/plumpton/2021-03-01/777465'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2018-11-13/714479'::text, 'https://www.racingpost.com/results/54/sandown/2019-12-07/744492'::text, 'https://www.racingpost.com/results/4/bangor-on-dee/2022-10-25/822558'::text, 'https://www.racingpost.com/results/67/stratford/2023-06-20/842207'::text, 'https://www.racingpost.com/results/23/haydock/2018-12-30/717697'::text, 'https://www.racingpost.com/results/44/plumpton/2022-04-18/807343'::text, 'https://www.racingpost.com/results/37/newcastle/2024-03-05/860189'::text, 'https://www.racingpost.com/results/27/kelso/2024-04-15/863253'::text, 'https://www.racingpost.com/results/35/market-rasen/2024-05-10/865428'::text])));


ALTER VIEW rp_raw.missing_links OWNER TO doadmin;

--
-- Name: todays_races; Type: VIEW; Schema: rp_raw; Owner: postgres
--

CREATE VIEW rp_raw.todays_races AS
 SELECT DISTINCT race_date,
    course,
    race_timestamp
   FROM rp_raw.todays_performance_data
  WHERE ((race_date)::date >= CURRENT_DATE)
  ORDER BY race_date, course, race_timestamp;


ALTER VIEW rp_raw.todays_races OWNER TO postgres;

--
-- Name: unioned_performance_data; Type: TABLE; Schema: rp_raw; Owner: postgres
--

CREATE TABLE rp_raw.unioned_performance_data (
    horse_id integer,
    race_id integer,
    race_date date,
    performance_metric character varying(100)
);


ALTER TABLE rp_raw.unioned_performance_data OWNER TO postgres;

--
-- Name: unmatched_dams; Type: VIEW; Schema: rp_raw; Owner: doadmin
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


ALTER VIEW rp_raw.unmatched_dams OWNER TO doadmin;

--
-- Name: unmatched_horses; Type: VIEW; Schema: rp_raw; Owner: doadmin
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


ALTER VIEW rp_raw.unmatched_horses OWNER TO doadmin;

--
-- Name: unmatched_jockeys; Type: VIEW; Schema: rp_raw; Owner: doadmin
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


ALTER VIEW rp_raw.unmatched_jockeys OWNER TO doadmin;

--
-- Name: unmatched_owners; Type: VIEW; Schema: rp_raw; Owner: doadmin
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
        ), unioned_data AS (
         SELECT historical_data.owner_name,
            historical_data.owner_id
           FROM historical_data
        UNION
         SELECT present_data.owner_name,
            present_data.owner_id
           FROM present_data
        )
 SELECT owner_name AS name,
    owner_id AS rp_id
   FROM unioned_data u;


ALTER VIEW rp_raw.unmatched_owners OWNER TO doadmin;

--
-- Name: unmatched_sires; Type: VIEW; Schema: rp_raw; Owner: doadmin
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


ALTER VIEW rp_raw.unmatched_sires OWNER TO doadmin;

--
-- Name: unmatched_trainers; Type: VIEW; Schema: rp_raw; Owner: doadmin
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


ALTER VIEW rp_raw.unmatched_trainers OWNER TO doadmin;

--
-- Name: missing_performance_data_vw; Type: VIEW; Schema: staging; Owner: doadmin
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


ALTER VIEW staging.missing_performance_data_vw OWNER TO doadmin;

--
-- Name: todays_transformed_performance_data; Type: TABLE; Schema: staging; Owner: doadmin
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


ALTER TABLE staging.todays_transformed_performance_data OWNER TO doadmin;

--
-- Name: todays_transformed_race_data; Type: TABLE; Schema: staging; Owner: doadmin
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


ALTER TABLE staging.todays_transformed_race_data OWNER TO doadmin;

--
-- Name: transformed_performance_data; Type: TABLE; Schema: staging; Owner: doadmin
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


ALTER TABLE staging.transformed_performance_data OWNER TO doadmin;

--
-- Name: transformed_race_data; Type: TABLE; Schema: staging; Owner: doadmin
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


ALTER TABLE staging.transformed_race_data OWNER TO doadmin;

--
-- Name: days_results_links; Type: TABLE; Schema: tf_raw; Owner: doadmin
--

CREATE TABLE tf_raw.days_results_links (
    date date,
    link text
);


ALTER TABLE tf_raw.days_results_links OWNER TO doadmin;

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
-- Name: unioned_performance_data_2010; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2010 FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');


--
-- Name: unioned_performance_data_2011; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2011 FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');


--
-- Name: unioned_performance_data_2012; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2012 FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');


--
-- Name: unioned_performance_data_2013; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2013 FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');


--
-- Name: unioned_performance_data_2014; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2014 FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');


--
-- Name: unioned_performance_data_2015; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2015 FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');


--
-- Name: unioned_performance_data_2016; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2016 FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');


--
-- Name: unioned_performance_data_2017; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2017 FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');


--
-- Name: unioned_performance_data_2018; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2018 FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');


--
-- Name: unioned_performance_data_2019; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2019 FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');


--
-- Name: unioned_performance_data_2020; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2020 FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');


--
-- Name: unioned_performance_data_2021; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2021 FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');


--
-- Name: unioned_performance_data_2022; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2022 FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');


--
-- Name: unioned_performance_data_2023; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2023 FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');


--
-- Name: unioned_performance_data_2024; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');


--
-- Name: unioned_performance_data_2025; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2025 FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');


--
-- Name: unioned_performance_data_2026; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');


--
-- Name: unioned_performance_data_2027; Type: TABLE ATTACH; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.unioned_performance_data ATTACH PARTITION public.unioned_performance_data_2027 FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');


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
-- Name: users id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


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
-- Name: race_data race_data_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.race_data
    ADD CONSTRAINT race_data_pkey PRIMARY KEY (race_id);


--
-- Name: owner rp_owner_unique_id; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.owner
    ADD CONSTRAINT rp_owner_unique_id UNIQUE (rp_id, name);


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
-- Name: todays_race_data todays_race_data_pkey; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.todays_race_data
    ADD CONSTRAINT todays_race_data_pkey PRIMARY KEY (race_id);


--
-- Name: todays_performance_data todays_unique_id_pd; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.todays_performance_data
    ADD CONSTRAINT todays_unique_id_pd UNIQUE (unique_id);


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
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: transformed_race_data race_id_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: doadmin
--

ALTER TABLE ONLY staging.transformed_race_data
    ADD CONSTRAINT race_id_stg_tns UNIQUE (race_id);


--
-- Name: todays_transformed_race_data race_id_todays_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: doadmin
--

ALTER TABLE ONLY staging.todays_transformed_race_data
    ADD CONSTRAINT race_id_todays_stg_tns UNIQUE (race_id);


--
-- Name: transformed_performance_data unique_id_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: doadmin
--

ALTER TABLE ONLY staging.transformed_performance_data
    ADD CONSTRAINT unique_id_stg_tns UNIQUE (unique_id);


--
-- Name: todays_transformed_performance_data unique_id_todays_stg_tns; Type: CONSTRAINT; Schema: staging; Owner: doadmin
--

ALTER TABLE ONLY staging.todays_transformed_performance_data
    ADD CONSTRAINT unique_id_todays_stg_tns UNIQUE (unique_id);


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
-- Name: idx_horse_id_upd; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_horse_id_upd ON ONLY public.unioned_performance_data USING btree (horse_id);


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

CREATE INDEX idx_race_data_course_id ON public.race_data USING btree (course_id);


--
-- Name: idx_race_data_meeting_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_data_meeting_id ON public.race_data USING btree (meeting_id);


--
-- Name: idx_race_data_race_date; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_data_race_date ON public.race_data USING btree (race_date);


--
-- Name: idx_race_data_race_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_data_race_id ON public.race_data USING btree (race_id);


--
-- Name: idx_race_date_upd; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_date_upd ON ONLY public.unioned_performance_data USING btree (race_date);


--
-- Name: idx_race_id_upd; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_race_id_upd ON ONLY public.unioned_performance_data USING btree (race_id);


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
-- Name: idx_todays_performance_data_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_course_id ON public.todays_performance_data USING btree (course_id);


--
-- Name: idx_todays_performance_data_dam_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_dam_id ON public.todays_performance_data USING btree (dam_id);


--
-- Name: idx_todays_performance_data_horse_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_horse_id ON public.todays_performance_data USING btree (horse_id);


--
-- Name: idx_todays_performance_data_jockey_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_jockey_id ON public.todays_performance_data USING btree (jockey_id);


--
-- Name: idx_todays_performance_data_owner_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_owner_id ON public.todays_performance_data USING btree (owner_id);


--
-- Name: idx_todays_performance_data_race_date; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_race_date ON public.todays_performance_data USING btree (race_date);


--
-- Name: idx_todays_performance_data_sire_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_sire_id ON public.todays_performance_data USING btree (sire_id);


--
-- Name: idx_todays_performance_data_trainer_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_trainer_id ON public.todays_performance_data USING btree (trainer_id);


--
-- Name: idx_todays_performance_data_unique_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_performance_data_unique_id ON public.todays_performance_data USING btree (unique_id);


--
-- Name: idx_todays_race_data_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_race_data_course_id ON public.todays_race_data USING btree (course_id);


--
-- Name: idx_todays_race_data_meeting_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_race_data_meeting_id ON public.todays_race_data USING btree (meeting_id);


--
-- Name: idx_todays_race_data_race_date; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_race_data_race_date ON public.todays_race_data USING btree (race_date);


--
-- Name: idx_todays_race_data_race_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_todays_race_data_race_id ON public.todays_race_data USING btree (race_id);


--
-- Name: idx_trainer_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_trainer_id ON public.trainer USING btree (rp_id);


--
-- Name: idx_trainer_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_trainer_name ON public.trainer USING btree (name);


--
-- Name: unioned_performance_data_2010_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2010_horse_id_idx ON public.unioned_performance_data_2010 USING btree (horse_id);


--
-- Name: unioned_performance_data_2010_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2010_race_date_idx ON public.unioned_performance_data_2010 USING btree (race_date);


--
-- Name: unioned_performance_data_2010_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2010_race_id_idx ON public.unioned_performance_data_2010 USING btree (race_id);


--
-- Name: unioned_performance_data_2011_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2011_horse_id_idx ON public.unioned_performance_data_2011 USING btree (horse_id);


--
-- Name: unioned_performance_data_2011_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2011_race_date_idx ON public.unioned_performance_data_2011 USING btree (race_date);


--
-- Name: unioned_performance_data_2011_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2011_race_id_idx ON public.unioned_performance_data_2011 USING btree (race_id);


--
-- Name: unioned_performance_data_2012_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2012_horse_id_idx ON public.unioned_performance_data_2012 USING btree (horse_id);


--
-- Name: unioned_performance_data_2012_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2012_race_date_idx ON public.unioned_performance_data_2012 USING btree (race_date);


--
-- Name: unioned_performance_data_2012_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2012_race_id_idx ON public.unioned_performance_data_2012 USING btree (race_id);


--
-- Name: unioned_performance_data_2013_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2013_horse_id_idx ON public.unioned_performance_data_2013 USING btree (horse_id);


--
-- Name: unioned_performance_data_2013_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2013_race_date_idx ON public.unioned_performance_data_2013 USING btree (race_date);


--
-- Name: unioned_performance_data_2013_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2013_race_id_idx ON public.unioned_performance_data_2013 USING btree (race_id);


--
-- Name: unioned_performance_data_2014_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2014_horse_id_idx ON public.unioned_performance_data_2014 USING btree (horse_id);


--
-- Name: unioned_performance_data_2014_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2014_race_date_idx ON public.unioned_performance_data_2014 USING btree (race_date);


--
-- Name: unioned_performance_data_2014_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2014_race_id_idx ON public.unioned_performance_data_2014 USING btree (race_id);


--
-- Name: unioned_performance_data_2015_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2015_horse_id_idx ON public.unioned_performance_data_2015 USING btree (horse_id);


--
-- Name: unioned_performance_data_2015_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2015_race_date_idx ON public.unioned_performance_data_2015 USING btree (race_date);


--
-- Name: unioned_performance_data_2015_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2015_race_id_idx ON public.unioned_performance_data_2015 USING btree (race_id);


--
-- Name: unioned_performance_data_2016_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2016_horse_id_idx ON public.unioned_performance_data_2016 USING btree (horse_id);


--
-- Name: unioned_performance_data_2016_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2016_race_date_idx ON public.unioned_performance_data_2016 USING btree (race_date);


--
-- Name: unioned_performance_data_2016_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2016_race_id_idx ON public.unioned_performance_data_2016 USING btree (race_id);


--
-- Name: unioned_performance_data_2017_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2017_horse_id_idx ON public.unioned_performance_data_2017 USING btree (horse_id);


--
-- Name: unioned_performance_data_2017_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2017_race_date_idx ON public.unioned_performance_data_2017 USING btree (race_date);


--
-- Name: unioned_performance_data_2017_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2017_race_id_idx ON public.unioned_performance_data_2017 USING btree (race_id);


--
-- Name: unioned_performance_data_2018_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2018_horse_id_idx ON public.unioned_performance_data_2018 USING btree (horse_id);


--
-- Name: unioned_performance_data_2018_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2018_race_date_idx ON public.unioned_performance_data_2018 USING btree (race_date);


--
-- Name: unioned_performance_data_2018_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2018_race_id_idx ON public.unioned_performance_data_2018 USING btree (race_id);


--
-- Name: unioned_performance_data_2019_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2019_horse_id_idx ON public.unioned_performance_data_2019 USING btree (horse_id);


--
-- Name: unioned_performance_data_2019_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2019_race_date_idx ON public.unioned_performance_data_2019 USING btree (race_date);


--
-- Name: unioned_performance_data_2019_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2019_race_id_idx ON public.unioned_performance_data_2019 USING btree (race_id);


--
-- Name: unioned_performance_data_2020_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2020_horse_id_idx ON public.unioned_performance_data_2020 USING btree (horse_id);


--
-- Name: unioned_performance_data_2020_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2020_race_date_idx ON public.unioned_performance_data_2020 USING btree (race_date);


--
-- Name: unioned_performance_data_2020_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2020_race_id_idx ON public.unioned_performance_data_2020 USING btree (race_id);


--
-- Name: unioned_performance_data_2021_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2021_horse_id_idx ON public.unioned_performance_data_2021 USING btree (horse_id);


--
-- Name: unioned_performance_data_2021_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2021_race_date_idx ON public.unioned_performance_data_2021 USING btree (race_date);


--
-- Name: unioned_performance_data_2021_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2021_race_id_idx ON public.unioned_performance_data_2021 USING btree (race_id);


--
-- Name: unioned_performance_data_2022_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2022_horse_id_idx ON public.unioned_performance_data_2022 USING btree (horse_id);


--
-- Name: unioned_performance_data_2022_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2022_race_date_idx ON public.unioned_performance_data_2022 USING btree (race_date);


--
-- Name: unioned_performance_data_2022_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2022_race_id_idx ON public.unioned_performance_data_2022 USING btree (race_id);


--
-- Name: unioned_performance_data_2023_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2023_horse_id_idx ON public.unioned_performance_data_2023 USING btree (horse_id);


--
-- Name: unioned_performance_data_2023_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2023_race_date_idx ON public.unioned_performance_data_2023 USING btree (race_date);


--
-- Name: unioned_performance_data_2023_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2023_race_id_idx ON public.unioned_performance_data_2023 USING btree (race_id);


--
-- Name: unioned_performance_data_2024_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2024_horse_id_idx ON public.unioned_performance_data_2024 USING btree (horse_id);


--
-- Name: unioned_performance_data_2024_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2024_race_date_idx ON public.unioned_performance_data_2024 USING btree (race_date);


--
-- Name: unioned_performance_data_2024_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2024_race_id_idx ON public.unioned_performance_data_2024 USING btree (race_id);


--
-- Name: unioned_performance_data_2025_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2025_horse_id_idx ON public.unioned_performance_data_2025 USING btree (horse_id);


--
-- Name: unioned_performance_data_2025_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2025_race_date_idx ON public.unioned_performance_data_2025 USING btree (race_date);


--
-- Name: unioned_performance_data_2025_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2025_race_id_idx ON public.unioned_performance_data_2025 USING btree (race_id);


--
-- Name: unioned_performance_data_2026_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2026_horse_id_idx ON public.unioned_performance_data_2026 USING btree (horse_id);


--
-- Name: unioned_performance_data_2026_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2026_race_date_idx ON public.unioned_performance_data_2026 USING btree (race_date);


--
-- Name: unioned_performance_data_2026_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2026_race_id_idx ON public.unioned_performance_data_2026 USING btree (race_id);


--
-- Name: unioned_performance_data_2027_horse_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2027_horse_id_idx ON public.unioned_performance_data_2027 USING btree (horse_id);


--
-- Name: unioned_performance_data_2027_race_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2027_race_date_idx ON public.unioned_performance_data_2027 USING btree (race_date);


--
-- Name: unioned_performance_data_2027_race_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX unioned_performance_data_2027_race_id_idx ON public.unioned_performance_data_2027 USING btree (race_id);


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
-- Name: idx_rp_raw_performance_data_owner_id; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_owner_id ON rp_raw.performance_data USING btree (owner_id);


--
-- Name: idx_rp_raw_performance_data_race_date; Type: INDEX; Schema: rp_raw; Owner: doadmin
--

CREATE INDEX idx_rp_raw_performance_data_race_date ON rp_raw.performance_data USING btree (race_date);


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
-- Name: unioned_performance_data_2010_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2010_horse_id_idx;


--
-- Name: unioned_performance_data_2010_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2010_race_date_idx;


--
-- Name: unioned_performance_data_2010_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2010_race_id_idx;


--
-- Name: unioned_performance_data_2011_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2011_horse_id_idx;


--
-- Name: unioned_performance_data_2011_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2011_race_date_idx;


--
-- Name: unioned_performance_data_2011_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2011_race_id_idx;


--
-- Name: unioned_performance_data_2012_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2012_horse_id_idx;


--
-- Name: unioned_performance_data_2012_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2012_race_date_idx;


--
-- Name: unioned_performance_data_2012_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2012_race_id_idx;


--
-- Name: unioned_performance_data_2013_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2013_horse_id_idx;


--
-- Name: unioned_performance_data_2013_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2013_race_date_idx;


--
-- Name: unioned_performance_data_2013_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2013_race_id_idx;


--
-- Name: unioned_performance_data_2014_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2014_horse_id_idx;


--
-- Name: unioned_performance_data_2014_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2014_race_date_idx;


--
-- Name: unioned_performance_data_2014_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2014_race_id_idx;


--
-- Name: unioned_performance_data_2015_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2015_horse_id_idx;


--
-- Name: unioned_performance_data_2015_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2015_race_date_idx;


--
-- Name: unioned_performance_data_2015_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2015_race_id_idx;


--
-- Name: unioned_performance_data_2016_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2016_horse_id_idx;


--
-- Name: unioned_performance_data_2016_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2016_race_date_idx;


--
-- Name: unioned_performance_data_2016_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2016_race_id_idx;


--
-- Name: unioned_performance_data_2017_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2017_horse_id_idx;


--
-- Name: unioned_performance_data_2017_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2017_race_date_idx;


--
-- Name: unioned_performance_data_2017_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2017_race_id_idx;


--
-- Name: unioned_performance_data_2018_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2018_horse_id_idx;


--
-- Name: unioned_performance_data_2018_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2018_race_date_idx;


--
-- Name: unioned_performance_data_2018_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2018_race_id_idx;


--
-- Name: unioned_performance_data_2019_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2019_horse_id_idx;


--
-- Name: unioned_performance_data_2019_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2019_race_date_idx;


--
-- Name: unioned_performance_data_2019_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2019_race_id_idx;


--
-- Name: unioned_performance_data_2020_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2020_horse_id_idx;


--
-- Name: unioned_performance_data_2020_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2020_race_date_idx;


--
-- Name: unioned_performance_data_2020_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2020_race_id_idx;


--
-- Name: unioned_performance_data_2021_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2021_horse_id_idx;


--
-- Name: unioned_performance_data_2021_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2021_race_date_idx;


--
-- Name: unioned_performance_data_2021_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2021_race_id_idx;


--
-- Name: unioned_performance_data_2022_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2022_horse_id_idx;


--
-- Name: unioned_performance_data_2022_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2022_race_date_idx;


--
-- Name: unioned_performance_data_2022_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2022_race_id_idx;


--
-- Name: unioned_performance_data_2023_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2023_horse_id_idx;


--
-- Name: unioned_performance_data_2023_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2023_race_date_idx;


--
-- Name: unioned_performance_data_2023_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2023_race_id_idx;


--
-- Name: unioned_performance_data_2024_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2024_horse_id_idx;


--
-- Name: unioned_performance_data_2024_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2024_race_date_idx;


--
-- Name: unioned_performance_data_2024_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2024_race_id_idx;


--
-- Name: unioned_performance_data_2025_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2025_horse_id_idx;


--
-- Name: unioned_performance_data_2025_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2025_race_date_idx;


--
-- Name: unioned_performance_data_2025_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2025_race_id_idx;


--
-- Name: unioned_performance_data_2026_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2026_horse_id_idx;


--
-- Name: unioned_performance_data_2026_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2026_race_date_idx;


--
-- Name: unioned_performance_data_2026_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2026_race_id_idx;


--
-- Name: unioned_performance_data_2027_horse_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_horse_id_upd ATTACH PARTITION public.unioned_performance_data_2027_horse_id_idx;


--
-- Name: unioned_performance_data_2027_race_date_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_date_upd ATTACH PARTITION public.unioned_performance_data_2027_race_date_idx;


--
-- Name: unioned_performance_data_2027_race_id_idx; Type: INDEX ATTACH; Schema: public; Owner: doadmin
--

ALTER INDEX public.idx_race_id_upd ATTACH PARTITION public.unioned_performance_data_2027_race_id_idx;


--
-- PostgreSQL database dump complete
--

