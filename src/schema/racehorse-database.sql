--
-- PostgreSQL database dump
--

-- Dumped from database version 16.1
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
-- Name: transformed; Type: SCHEMA; Schema: -; Owner: doadmin
--

CREATE SCHEMA transformed;


ALTER SCHEMA transformed OWNER TO doadmin;

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


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
    
    INSERT INTO staging.dam (id, name, tf_id)
    SELECT rp_dam_id, rp_dam_name, tf_dam_id
    FROM matches;
	
	INSERT INTO public.dam (id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.dam
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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
    
    INSERT INTO staging.horse (id, name, tf_id)
    SELECT rp_horse_id, rp_horse_name, tf_horse_id
    FROM matches;
	
	INSERT INTO public.horse(id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.horse
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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
    
    INSERT INTO staging.sire (id, name, tf_id)
    SELECT rp_sire_id, rp_sire_name, tf_sire_id
    FROM matches;
	
	INSERT INTO public.sire (id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.sire
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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
        rp_horse_id,
        rp_horse_name,
        rp_horse_age,
        rp_jockey_id,
        rp_jockey_name,
        rp_jockey_claim,
        rp_trainer_id,
        rp_trainer_name,
        rp_owner_id,
        rp_owner_name,
        rp_horse_weight,
        rp_or_value,
        rp_finishing_position,
        rp_draw,
        rp_ts_value,
        rp_rpr_value,
        rp_horse_price,
        rp_extra_weight,
        rp_headgear,
        rp_comment,
        rp_sire_name,
        rp_sire_id,
        rp_dam_name,
        rp_dam_id,
        rp_dams_sire,
        rp_dams_sire_id,
        rp_race_date,
        rp_race_title,
        rp_race_time,
        rp_race_timestamp,
        rp_conditions,
        rp_distance,
        rp_going,
        rp_winning_time,
        rp_number_of_runners,
        rp_total_prize_money,
        rp_first_place_prize_money,
        rp_currency,
        rp_course_id,
        rp_course_name,
        rp_course,
        rp_race_id,
        rp_country,
        rp_surface,
        rp_unique_id,
        tf_tf_rating,
        tf_tf_speed_figure,
        tf_draw,
        tf_finishing_position,
        tf_horse_age,
        tf_equipment,
        tf_official_rating,
        tf_fractional_price,
        tf_betfair_win_sp,
        tf_betfair_place_sp,
        tf_in_play_prices,
        tf_tf_comment,
        tf_race_date,
        tf_race_time,
        tf_race_timestamp,
        tf_race,
        tf_race_id,
        tf_distance,
        tf_going,
        tf_prize,
        tf_hcap_range,
        tf_age_range,
        tf_race_type,
        tf_main_race_comment,
        tf_unique_id,
		created_at,
		rp_meeting_id
    )

WITH new_rp_records AS (
    SELECT *
	FROM staging.missing_performance_data
),
joined_data AS (
    SELECT 
        nrr.*,
		tf.tf_rating as tf_tf_rating,
		tf.tf_speed_figure as tf_tf_speed_figure,
		tf.draw as tf_draw,
		tf.finishing_position as tf_finishing_position,
		tf.horse_age as tf_horse_age,
		tf.equipment as tf_equipment,
		tf.official_rating as tf_official_rating,
		tf.fractional_price as tf_fractional_price,
		tf.betfair_win_sp as tf_betfair_win_sp,
		tf.betfair_place_sp as tf_betfair_place_sp,
		tf.in_play_prices as tf_in_play_prices,
		tf.tf_comment as tf_tf_comment,
		tf.race_date as tf_race_date,
		tf.race_time as tf_race_time,
		tf.race_timestamp as tf_race_timestamp,
		tf.race as tf_race,
		tf.race_id as tf_race_id,
		tf.distance as tf_distance,
		tf.going as tf_going,
		tf.prize as tf_prize,
		tf.hcap_range as tf_hcap_range,
		tf.age_range as tf_age_range,
		tf.race_type as tf_race_type,
		tf.main_race_comment as tf_main_race_comment,
		tf.unique_id as tf_unique_id,
        ROW_NUMBER() OVER(PARTITION BY nrr.rp_unique_id ORDER BY nrr.rp_unique_id) AS rn
    FROM new_rp_records nrr
	LEFT JOIN course c ON nrr.rp_course_id = c.rp_course_id
	LEFT JOIN horse h ON nrr.rp_horse_id = h.id
	LEFT JOIN sire s ON nrr.rp_sire_id = s.id
	LEFT JOIN dam d ON nrr.rp_dam_id = d.id
	LEFT JOIN trainer t ON nrr.rp_trainer_id = t.id
	LEFT JOIN jockey j ON nrr.rp_jockey_id = j.id
    LEFT JOIN tf_raw.performance_data tf ON tf.horse_id = h.tf_id
		AND tf.jockey_id = j.tf_id
		AND tf.trainer_id = t.tf_id
		AND tf.dam_id = d.tf_id
		AND tf.sire_id = s.tf_id
		AND tf.race_date = nrr.rp_race_date
		AND tf.course_id = c.tf_course_id
    WHERE tf.unique_id IS NOT NULL
)
SELECT rp_horse_id,
        rp_horse_name,
        rp_horse_age,
        rp_jockey_id,
        rp_jockey_name,
        rp_jockey_claim,
        rp_trainer_id,
        rp_trainer_name,
        rp_owner_id,
        rp_owner_name,
        rp_horse_weight,
        rp_or_value,
        rp_finishing_position,
        rp_draw,
        rp_ts_value,
        rp_rpr_value,
        rp_horse_price,
        rp_extra_weight,
        rp_headgear,
        rp_comment,
        rp_sire_name,
        rp_sire_id,
        rp_dam_name,
        rp_dam_id,
        rp_dams_sire,
        rp_dams_sire_id,
        rp_race_date,
        rp_race_title,
        rp_race_time,
        rp_race_timestamp,
        rp_conditions,
        rp_distance,
        rp_going,
        rp_winning_time,
        rp_number_of_runners,
        rp_total_prize_money,
        rp_first_place_prize_money,
        rp_currency,
        rp_course_id,
        rp_course_name,
        rp_course,
        rp_race_id,
        rp_country,
        rp_surface,
        rp_unique_id,
        tf_tf_rating,
        tf_tf_speed_figure,
        tf_draw,
        tf_finishing_position,
        tf_horse_age,
        tf_equipment,
        tf_official_rating,
        tf_fractional_price,
        tf_betfair_win_sp,
        tf_betfair_place_sp,
        tf_in_play_prices,
        tf_tf_comment,
        tf_race_date,
        tf_race_time,
        tf_race_timestamp,
        tf_race,
        tf_race_id,
        tf_distance,
        tf_going,
        tf_prize,
        tf_hcap_range,
        tf_age_range,
        tf_race_type,
        tf_main_race_comment,
        tf_unique_id,
		now(),
		rp_meeting_id
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

	INSERT INTO public.dam (id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.dam
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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

	INSERT INTO public.horse (id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.horse
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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

	INSERT INTO public.jockey (id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.jockey
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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

	INSERT INTO public.sire (id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.sire
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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

	INSERT INTO public.trainer (id, name, tf_id)
    SELECT id, name, tf_id
    FROM staging.trainer
	ON CONFLICT (id, tf_id) DO NOTHING;
	
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
with owners as
(	select distinct  rp.owner_id, rp.owner_name  from rp_raw.performance_data rp
	left join public.owner o
	on rp.owner_id = o.id
	where o.name is null)
	
	INSERT INTO public.owner (id, name)
    SELECT owner_id, owner_name
    FROM owners
	ON CONFLICT (id, name) DO NOTHING;
END;
$$;


ALTER PROCEDURE staging.insert_owner_data() OWNER TO doadmin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: course; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.course (
    course_name character varying(132),
    course_id character varying(4),
    rp_course_name character varying(132),
    rp_course_id character varying(4),
    tf_course_name character varying(132),
    tf_course_id character varying(4),
    country_id character varying(4)
);


ALTER TABLE public.course OWNER TO doadmin;

--
-- Name: dam; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.dam (
    id character varying(132),
    name character varying(132),
    tf_id character varying(32)
);


ALTER TABLE public.dam OWNER TO doadmin;

--
-- Name: horse; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.horse (
    id character varying(132),
    name character varying(132),
    tf_id character varying(32)
);


ALTER TABLE public.horse OWNER TO doadmin;

--
-- Name: jockey; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.jockey (
    id character varying(132),
    name character varying(132),
    tf_id character varying(32)
);


ALTER TABLE public.jockey OWNER TO doadmin;

--
-- Name: sire; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.sire (
    id character varying(132),
    name character varying(132),
    tf_id character varying(32)
);


ALTER TABLE public.sire OWNER TO doadmin;

--
-- Name: trainer; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.trainer (
    id character varying(132),
    name character varying(132),
    tf_id character varying(32)
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
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (rp_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_course_id)::text)));


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
    d.id,
    d.name,
    d.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.dam d ON (((be.dam_id)::text = (d.id)::text)))
  WHERE (d.id IS NULL);


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
    h.id,
    h.name,
    h.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.horse h ON (((be.horse_id)::text = (h.id)::text)))
  WHERE (h.id IS NULL);


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
    j.id,
    j.name,
    j.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.jockey j ON (((be.jockey_id)::text = (j.id)::text)))
  WHERE (j.id IS NULL);


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
    s.id,
    s.name,
    s.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.sire s ON (((be.sire_id)::text = (s.id)::text)))
  WHERE (s.id IS NULL);


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
    t.id,
    t.name,
    t.tf_id
   FROM (rp_raw.base_formatted_entities be
     LEFT JOIN public.trainer t ON (((be.trainer_id)::text = (t.id)::text)))
  WHERE (t.id IS NULL);


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
-- Name: unmatched_dams; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_dams AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN ((pd.finishing_position)::text ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace((pd.sire_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace((pd.dam_name)::text, '\s*\([^\)]*\)|\s*\d+[A-Za-z]*'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
     LEFT JOIN public.dam d ON (((pd.dam_id)::text = (d.tf_id)::text)))
  WHERE (d.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_dams OWNER TO doadmin;

--
-- Name: unmatched_horses; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_horses AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
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
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date,
    c.course_name,
    c.course_id,
    c.rp_course_name,
    c.rp_course_id,
    c.tf_course_name,
    c.tf_course_id,
    c.country_id
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
     LEFT JOIN public.horse h ON (((pd.horse_id)::text = (h.tf_id)::text)))
  WHERE (h.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_horses OWNER TO doadmin;

--
-- Name: unmatched_jockeys; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_jockeys AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
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
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
     LEFT JOIN public.jockey j ON (((pd.jockey_id)::text = (j.tf_id)::text)))
  WHERE (j.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_jockeys OWNER TO doadmin;

--
-- Name: unmatched_sires; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_sires AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
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
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
     LEFT JOIN public.sire s ON (((pd.sire_id)::text = (s.tf_id)::text)))
  WHERE (s.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_sires OWNER TO doadmin;

--
-- Name: unmatched_trainers; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.unmatched_trainers AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
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
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
     LEFT JOIN public.trainer j ON (((pd.trainer_id)::text = (j.tf_id)::text)))
  WHERE (j.tf_id IS NULL);


ALTER VIEW tf_raw.unmatched_trainers OWNER TO doadmin;

--
-- Name: missing_entity_counts; Type: VIEW; Schema: metrics; Owner: doadmin
--

CREATE VIEW metrics.missing_entity_counts AS
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


ALTER VIEW metrics.missing_entity_counts OWNER TO doadmin;

--
-- Name: record_count_differences; Type: VIEW; Schema: metrics; Owner: doadmin
--

CREATE VIEW metrics.record_count_differences AS
 WITH rp_course_counts AS (
         SELECT c.course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.course_id
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_course_id)::text)))
          GROUP BY c.course_name, pd.race_date, c.course_id
        ), tf_course_counts AS (
         SELECT c.course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.course_id
           FROM (tf_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
          GROUP BY c.course_name, pd.race_date, c.course_id
        )
 SELECT COALESCE(rp.course_name, tf.course_name) AS course,
    COALESCE(rp.race_date, tf.race_date) AS race_date,
    rp.num_records AS rp_num_records,
    tf.num_records AS tf_num_records
   FROM (rp_course_counts rp
     JOIN tf_course_counts tf ON ((((rp.race_date)::text = (tf.race_date)::text) AND ((rp.course_id)::text = (tf.course_id)::text))))
  WHERE ((rp.num_records <> tf.num_records) OR (rp.num_records IS NULL) OR (tf.num_records IS NULL))
  ORDER BY COALESCE(rp.race_date, tf.race_date) DESC, COALESCE(rp.course_name, tf.course_name), rp.course_id;


ALTER VIEW metrics.record_count_differences OWNER TO doadmin;

--
-- Name: record_count_differences_gt_2010; Type: VIEW; Schema: metrics; Owner: doadmin
--

CREATE VIEW metrics.record_count_differences_gt_2010 AS
 SELECT course,
    race_date,
    rp_num_records,
    tf_num_records
   FROM metrics.record_count_differences
  WHERE ((race_date)::date > '2010-01-01'::date);


ALTER VIEW metrics.record_count_differences_gt_2010 OWNER TO doadmin;

--
-- Name: country; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.country (
    country_id character varying(4),
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
-- Name: owner; Type: TABLE; Schema: public; Owner: doadmin
--

CREATE TABLE public.owner (
    id text,
    name text
);


ALTER TABLE public.owner OWNER TO doadmin;

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
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (rp_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_course_id)::text)));


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
    id text,
    name text,
    tf_id text
);


ALTER TABLE staging.dam OWNER TO doadmin;

--
-- Name: horse; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.horse (
    id text,
    name text,
    tf_id text
);


ALTER TABLE staging.horse OWNER TO doadmin;

--
-- Name: jockey; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.jockey (
    id text,
    name text,
    tf_id text
);


ALTER TABLE staging.jockey OWNER TO doadmin;

--
-- Name: joined_performance_data; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.joined_performance_data (
    rp_horse_id character varying(32),
    rp_horse_name character varying(132),
    rp_horse_age character varying(16),
    rp_jockey_id character varying(32),
    rp_jockey_name character varying(132),
    rp_jockey_claim character varying(16),
    rp_trainer_id character varying(32),
    rp_trainer_name character varying(132),
    rp_owner_id character varying(32),
    rp_owner_name character varying(132),
    rp_horse_weight character varying(16),
    rp_or_value character varying(16),
    rp_finishing_position character varying(16),
    rp_draw character varying(16),
    rp_ts_value character varying(16),
    rp_rpr_value character varying(16),
    rp_horse_price character varying(16),
    rp_extra_weight numeric,
    rp_headgear character varying(16),
    rp_comment text,
    rp_sire_name character varying(132),
    rp_sire_id character varying(32),
    rp_dam_name character varying(132),
    rp_dam_id character varying(32),
    rp_dams_sire character varying(132),
    rp_dams_sire_id character varying(32),
    rp_race_date character varying(32),
    rp_race_title text,
    rp_race_time character varying(32),
    rp_race_timestamp timestamp without time zone,
    rp_conditions character varying(32),
    rp_distance character varying(32),
    rp_going character varying(32),
    rp_winning_time character varying(32),
    rp_number_of_runners character varying(32),
    rp_total_prize_money integer,
    rp_first_place_prize_money integer,
    rp_currency character varying(16),
    rp_course_id character varying(32),
    rp_course_name character varying(132),
    rp_course character varying(132),
    rp_race_id character varying(32),
    rp_country character varying(16),
    rp_surface character varying(16),
    rp_unique_id character varying(132),
    tf_tf_rating character varying(16),
    tf_tf_speed_figure character varying(16),
    tf_draw character varying(16),
    tf_finishing_position character varying(16),
    tf_horse_age character varying(16),
    tf_equipment character varying(16),
    tf_official_rating character varying(16),
    tf_fractional_price character varying(64),
    tf_betfair_win_sp character varying(16),
    tf_betfair_place_sp character varying(16),
    tf_in_play_prices character varying(16),
    tf_tf_comment text,
    tf_race_date character varying(32),
    tf_race_time character varying(32),
    tf_race_timestamp timestamp without time zone,
    tf_race text,
    tf_race_id character varying(132),
    tf_distance character varying(32),
    tf_going character varying(16),
    tf_prize character varying(16),
    tf_hcap_range character varying(16),
    tf_age_range character varying(16),
    tf_race_type character varying(16),
    tf_main_race_comment text,
    tf_unique_id character varying(132),
    created_at timestamp without time zone,
    rp_meeting_id character varying(132)
);


ALTER TABLE staging.joined_performance_data OWNER TO doadmin;

--
-- Name: missing_performance_data; Type: VIEW; Schema: staging; Owner: doadmin
--

CREATE VIEW staging.missing_performance_data AS
 SELECT DISTINCT ON (rp.unique_id) rp.horse_id AS rp_horse_id,
    rp.horse_name AS rp_horse_name,
    rp.horse_age AS rp_horse_age,
    rp.jockey_id AS rp_jockey_id,
    rp.jockey_name AS rp_jockey_name,
    rp.jockey_claim AS rp_jockey_claim,
    rp.trainer_id AS rp_trainer_id,
    rp.trainer_name AS rp_trainer_name,
    rp.owner_id AS rp_owner_id,
    rp.owner_name AS rp_owner_name,
    rp.horse_weight AS rp_horse_weight,
    rp.or_value AS rp_or_value,
    rp.finishing_position AS rp_finishing_position,
    rp.draw AS rp_draw,
    rp.ts_value AS rp_ts_value,
    rp.rpr_value AS rp_rpr_value,
    rp.horse_price AS rp_horse_price,
    rp.extra_weight AS rp_extra_weight,
    rp.headgear AS rp_headgear,
    rp.comment AS rp_comment,
    rp.sire_name AS rp_sire_name,
    rp.sire_id AS rp_sire_id,
    rp.dam_name AS rp_dam_name,
    rp.dam_id AS rp_dam_id,
    rp.dams_sire AS rp_dams_sire,
    rp.dams_sire_id AS rp_dams_sire_id,
    rp.race_date AS rp_race_date,
    rp.race_title AS rp_race_title,
    rp.race_time AS rp_race_time,
    rp.race_timestamp AS rp_race_timestamp,
    rp.conditions AS rp_conditions,
    rp.distance AS rp_distance,
    rp.going AS rp_going,
    rp.winning_time AS rp_winning_time,
    rp.number_of_runners AS rp_number_of_runners,
    rp.total_prize_money AS rp_total_prize_money,
    rp.first_place_prize_money AS rp_first_place_prize_money,
    rp.currency AS rp_currency,
    rp.course_id AS rp_course_id,
    rp.course_name AS rp_course_name,
    rp.course AS rp_course,
    rp.race_id AS rp_race_id,
    rp.country AS rp_country,
    rp.surface AS rp_surface,
    rp.unique_id AS rp_unique_id,
    rp.meeting_id
   FROM (rp_raw.performance_data rp
     LEFT JOIN staging.joined_performance_data sjpd ON (((rp.unique_id)::text = (sjpd.rp_unique_id)::text)))
  WHERE ((sjpd.rp_unique_id IS NULL) AND (rp.race_timestamp <> '2018-12-02 15:05:00'::timestamp without time zone));


ALTER VIEW staging.missing_performance_data OWNER TO doadmin;

--
-- Name: sire; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.sire (
    id text,
    name text,
    tf_id text
);


ALTER TABLE staging.sire OWNER TO doadmin;

--
-- Name: trainer; Type: TABLE; Schema: staging; Owner: doadmin
--

CREATE TABLE staging.trainer (
    id text,
    name text,
    tf_id text
);


ALTER TABLE staging.trainer OWNER TO doadmin;

--
-- Name: all_entities; Type: VIEW; Schema: tf_raw; Owner: doadmin
--

CREATE VIEW tf_raw.all_entities AS
 WITH rp_raw_entities AS (
         SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
            regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
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
            c.course_id,
            pd.unique_id,
            (pd.race_timestamp)::date AS race_date
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
        ), tf_raw_entities AS (
         SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
            regexp_replace(lower(regexp_replace((pd.horse_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            regexp_replace(lower(regexp_replace((pd.jockey_name)::text, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            regexp_replace(lower(regexp_replace((pd.trainer_name)::text, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
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
            c.course_id,
            pd.unique_id,
            (pd.race_timestamp)::date AS race_date
           FROM (tf_raw.performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)))
        ), joined_matches AS (
         SELECT rp.filtered_horse_name AS rp_filtered_horse_name,
            rp.horse_id AS rp_horse_id,
            rp.horse_age AS rp_horse_age,
            rp.jockey_id AS rp_jockey_id,
            rp.jockey_name AS rp_jockey_name,
            rp.filtered_jockey_name AS rp_filtered_jockey_name,
            rp.trainer_id AS rp_trainer_id,
            rp.trainer_name AS rp_trainer_name,
            rp.filtered_trainer_name AS rp_filtered_trainer_name,
            rp.converted_finishing_position AS rp_converted_finishing_position,
            rp.sire_name AS rp_sire_name,
            rp.filtered_sire_name AS rp_filtered_sire_name,
            rp.sire_id AS rp_sire_id,
            rp.dam_name AS rp_dam_name,
            rp.filtered_dam_name AS rp_filtered_dam_name,
            rp.dam_id AS rp_dam_id,
            rp.race_timestamp AS rp_race_timestamp,
            rp.course_id AS rp_course_id,
            rp.unique_id AS rp_unique_id,
            rp.race_date AS rp_race_date,
            tf.filtered_horse_name AS tf_filtered_horse_name,
            tf.horse_id AS tf_horse_id,
            tf.horse_age AS tf_horse_age,
            tf.jockey_id AS tf_jockey_id,
            tf.jockey_name AS tf_jockey_name,
            tf.filtered_jockey_name AS tf_filtered_jockey_name,
            tf.trainer_id AS tf_trainer_id,
            tf.trainer_name AS tf_trainer_name,
            tf.filtered_trainer_name AS tf_filtered_trainer_name,
            tf.converted_finishing_position AS tf_converted_finishing_position,
            tf.sire_name AS tf_sire_name,
            tf.filtered_sire_name AS tf_filtered_sire_name,
            tf.sire_id AS tf_sire_id,
            tf.dam_name AS tf_dam_name,
            tf.filtered_dam_name AS tf_filtered_dam_name,
            tf.dam_id AS tf_dam_id,
            tf.race_timestamp AS tf_race_timestamp,
            tf.course_id AS tf_course_id,
            tf.unique_id AS tf_unique_id,
            tf.race_date AS tf_race_date
           FROM (rp_raw_entities rp
             LEFT JOIN tf_raw_entities tf ON (((rp.race_timestamp = tf.race_timestamp) AND ((rp.course_id)::text = (tf.course_id)::text) AND (rp.converted_finishing_position = tf.converted_finishing_position) AND ((rp.horse_age)::text = (tf.horse_age)::text))))
          WHERE (tf.race_timestamp IS NOT NULL)
        )
 SELECT rp_filtered_horse_name,
    rp_horse_id,
    rp_horse_age,
    rp_jockey_id,
    rp_jockey_name,
    rp_filtered_jockey_name,
    rp_trainer_id,
    rp_trainer_name,
    rp_filtered_trainer_name,
    rp_converted_finishing_position,
    rp_sire_name,
    rp_filtered_sire_name,
    rp_sire_id,
    rp_dam_name,
    rp_filtered_dam_name,
    rp_dam_id,
    rp_race_timestamp,
    rp_course_id,
    rp_unique_id,
    rp_race_date,
    tf_filtered_horse_name,
    tf_horse_id,
    tf_horse_age,
    tf_jockey_id,
    tf_jockey_name,
    tf_filtered_jockey_name,
    tf_trainer_id,
    tf_trainer_name,
    tf_filtered_trainer_name,
    tf_converted_finishing_position,
    tf_sire_name,
    tf_filtered_sire_name,
    tf_sire_id,
    tf_dam_name,
    tf_filtered_dam_name,
    tf_dam_id,
    tf_race_timestamp,
    tf_course_id,
    tf_unique_id,
    tf_race_date
   FROM joined_matches;


ALTER VIEW tf_raw.all_entities OWNER TO doadmin;

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
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (tf_raw.performance_data pd
     LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_course_id)::text)));


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
-- Name: owner owner_name_id; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.owner
    ADD CONSTRAINT owner_name_id UNIQUE (id, name);


--
-- Name: dam rp_tf_dam_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.dam
    ADD CONSTRAINT rp_tf_dam_unique_id_v2 UNIQUE (id, tf_id);


--
-- Name: horse rp_tf_horse_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.horse
    ADD CONSTRAINT rp_tf_horse_unique_id_v2 UNIQUE (id, tf_id);


--
-- Name: jockey rp_tf_jockey_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.jockey
    ADD CONSTRAINT rp_tf_jockey_unique_id_v2 UNIQUE (id, tf_id);


--
-- Name: sire rp_tf_sire_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.sire
    ADD CONSTRAINT rp_tf_sire_unique_id_v2 UNIQUE (id, tf_id);


--
-- Name: trainer rp_tf_trainer_unique_id_v2; Type: CONSTRAINT; Schema: public; Owner: doadmin
--

ALTER TABLE ONLY public.trainer
    ADD CONSTRAINT rp_tf_trainer_unique_id_v2 UNIQUE (id, tf_id);


--
-- Name: joined_performance_data rp_tf_unique_id_stg_jnd; Type: CONSTRAINT; Schema: staging; Owner: doadmin
--

ALTER TABLE ONLY staging.joined_performance_data
    ADD CONSTRAINT rp_tf_unique_id_stg_jnd UNIQUE (rp_unique_id, tf_unique_id);


--
-- Name: idx_course_rp_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_course_rp_course_id ON public.course USING btree (rp_course_id);


--
-- Name: idx_course_tf_course_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_course_tf_course_id ON public.course USING btree (tf_course_id);


--
-- Name: idx_dam_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_dam_id ON public.dam USING btree (id);


--
-- Name: idx_dam_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_dam_name ON public.dam USING btree (name);


--
-- Name: idx_horse_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_horse_id ON public.horse USING btree (id);


--
-- Name: idx_horse_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_horse_name ON public.horse USING btree (name);


--
-- Name: idx_jockey_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_jockey_id ON public.jockey USING btree (id);


--
-- Name: idx_jockey_name; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_jockey_name ON public.jockey USING btree (name);


--
-- Name: idx_sire_id; Type: INDEX; Schema: public; Owner: doadmin
--

CREATE INDEX idx_sire_id ON public.sire USING btree (id);


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

CREATE INDEX idx_trainer_id ON public.trainer USING btree (id);


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
-- Name: idx_stg_performance_data_rp_unique_id; Type: INDEX; Schema: staging; Owner: doadmin
--

CREATE INDEX idx_stg_performance_data_rp_unique_id ON staging.joined_performance_data USING btree (rp_unique_id);


--
-- Name: idx_stg_performance_data_tf_unique_id; Type: INDEX; Schema: staging; Owner: doadmin
--

CREATE INDEX idx_stg_performance_data_tf_unique_id ON staging.joined_performance_data USING btree (tf_unique_id);


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

