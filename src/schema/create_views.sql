CREATE VIEW rp_raw.base_formatted_entities AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*I\s*$|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*I\s*$'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*I\s*$|\s*\d+[A-Za-z]*'::text, ''::text, 'gi'::text)), '[\s]+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (rp_raw.performance_data pd
     LEFT JOIN public.course c ON ((pd.course_id = (c.rp_course_id)::text)));

ALTER VIEW rp_raw.base_formatted_entities OWNER TO doadmin;

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
     LEFT JOIN public.dam d ON ((be.dam_id = (d.id)::text)))
  WHERE (d.id IS NULL);

ALTER VIEW rp_raw.unmatched_dams OWNER TO doadmin;

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
     LEFT JOIN public.horse h ON ((be.horse_id = (h.id)::text)))
  WHERE (h.id IS NULL);

ALTER VIEW rp_raw.unmatched_horses OWNER TO doadmin;

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
     LEFT JOIN public.jockey j ON ((be.jockey_id = (j.id)::text)))
  WHERE (j.id IS NULL);

ALTER VIEW rp_raw.unmatched_jockeys OWNER TO doadmin;

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
     LEFT JOIN public.sire s ON ((be.sire_id = (s.id)::text)))
  WHERE (s.id IS NULL);

ALTER VIEW rp_raw.unmatched_sires OWNER TO doadmin;

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
     LEFT JOIN public.trainer t ON ((be.trainer_id = (t.id)::text)))
  WHERE (t.id IS NULL);

ALTER VIEW rp_raw.unmatched_trainers OWNER TO doadmin;

CREATE VIEW tf_raw.unmatched_dams AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^\)]*\)|\s*\d+[A-Za-z]*'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
     LEFT JOIN public.dam d ON ((pd.dam_id = (d.tf_id)::text)))
  WHERE (d.tf_id IS NULL);

ALTER VIEW tf_raw.unmatched_dams OWNER TO doadmin;

CREATE VIEW tf_raw.unmatched_horses AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
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
     LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
     LEFT JOIN public.horse h ON ((pd.horse_id = (h.tf_id)::text)))
  WHERE (h.tf_id IS NULL);

ALTER VIEW tf_raw.unmatched_horses OWNER TO doadmin;

CREATE VIEW tf_raw.unmatched_jockeys AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
     LEFT JOIN public.jockey j ON ((pd.jockey_id = (j.tf_id)::text)))
  WHERE (j.tf_id IS NULL);

ALTER VIEW tf_raw.unmatched_jockeys OWNER TO doadmin;

CREATE VIEW tf_raw.unmatched_sires AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
     LEFT JOIN public.sire s ON ((pd.sire_id = (s.tf_id)::text)))
  WHERE (s.tf_id IS NULL);

ALTER VIEW tf_raw.unmatched_sires OWNER TO doadmin;

CREATE VIEW tf_raw.unmatched_trainers AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM ((tf_raw.performance_data pd
     LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
     LEFT JOIN public.trainer j ON ((pd.trainer_id = (j.tf_id)::text)))
  WHERE (j.tf_id IS NULL);

ALTER VIEW tf_raw.unmatched_trainers OWNER TO doadmin;

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

CREATE VIEW metrics.record_count_differences AS
 WITH rp_course_counts AS (
         SELECT c.course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.course_id
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.course c ON ((pd.course_id = (c.rp_course_id)::text)))
          GROUP BY c.course_name, pd.race_date, c.course_id
        ), tf_course_counts AS (
         SELECT c.course_name,
            pd.race_date,
            count(DISTINCT pd.unique_id) AS num_records,
            c.course_id
           FROM (tf_raw.performance_data pd
             LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
          GROUP BY c.course_name, pd.race_date, c.course_id
        )
 SELECT COALESCE(rp.course_name, tf.course_name) AS course,
    COALESCE(rp.race_date, tf.race_date) AS race_date,
    rp.num_records AS rp_num_records,
    tf.num_records AS tf_num_records
   FROM (rp_course_counts rp
     JOIN tf_course_counts tf ON (((rp.race_date = tf.race_date) AND ((rp.course_id)::text = (tf.course_id)::text))))
  WHERE ((rp.num_records <> tf.num_records) OR (rp.num_records IS NULL) OR (tf.num_records IS NULL))
  ORDER BY COALESCE(rp.race_date, tf.race_date) DESC, COALESCE(rp.course_name, tf.course_name), rp.course_id;

ALTER VIEW metrics.record_count_differences OWNER TO doadmin;

CREATE VIEW metrics.record_count_differences_gt_2010 AS
 SELECT course,
    race_date,
    rp_num_records,
    tf_num_records
   FROM metrics.record_count_differences
  WHERE ((race_date)::date > '2010-01-01'::date);

ALTER VIEW metrics.record_count_differences_gt_2010 OWNER TO doadmin;

CREATE VIEW rp_raw.formatted_rp_entities AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (rp_raw.performance_data pd
     LEFT JOIN public.course c ON ((pd.course_id = (c.rp_course_id)::text)));

ALTER VIEW rp_raw.formatted_rp_entities OWNER TO doadmin;

CREATE VIEW rp_raw.missing_dates AS
 SELECT gs.date
   FROM (generate_series(('2005-01-01'::date)::timestamp without time zone, ((now())::date - '3 days'::interval), '1 day'::interval) gs(date)
     LEFT JOIN ( SELECT DISTINCT days_results_links.date
           FROM rp_raw.days_results_links) distinct_dates ON ((gs.date = distinct_dates.date)))
  WHERE ((distinct_dates.date IS NULL) AND (NOT (gs.date = ANY (ARRAY[('2020-04-06'::date)::timestamp without time zone, ('2008-12-25'::date)::timestamp without time zone, ('2013-03-29'::date)::timestamp without time zone, ('2010-01-06'::date)::timestamp without time zone, ('2008-12-24'::date)::timestamp without time zone, ('2009-02-02'::date)::timestamp without time zone, ('2007-12-25'::date)::timestamp without time zone, ('2007-12-24'::date)::timestamp without time zone, ('2005-12-25'::date)::timestamp without time zone, ('2005-12-24'::date)::timestamp without time zone, ('2009-12-25'::date)::timestamp without time zone, ('2009-12-24'::date)::timestamp without time zone, ('2006-12-25'::date)::timestamp without time zone, ('2006-12-24'::date)::timestamp without time zone, ('2020-04-03'::date)::timestamp without time zone, ('2020-04-02'::date)::timestamp without time zone, ('2015-12-25'::date)::timestamp without time zone, ('2015-12-24'::date)::timestamp without time zone, ('2016-12-25'::date)::timestamp without time zone, ('2016-12-24'::date)::timestamp without time zone, ('2017-12-25'::date)::timestamp without time zone, ('2017-12-24'::date)::timestamp without time zone, ('2018-12-25'::date)::timestamp without time zone, ('2018-12-24'::date)::timestamp without time zone, ('2019-12-25'::date)::timestamp without time zone, ('2019-12-24'::date)::timestamp without time zone, ('2020-12-25'::date)::timestamp without time zone, ('2020-12-24'::date)::timestamp without time zone, ('2021-12-25'::date)::timestamp without time zone, ('2021-12-24'::date)::timestamp without time zone, ('2022-12-25'::date)::timestamp without time zone, ('2022-12-24'::date)::timestamp without time zone, ('2023-12-25'::date)::timestamp without time zone, ('2023-12-24'::date)::timestamp without time zone]))))
  ORDER BY gs.date DESC;

ALTER VIEW rp_raw.missing_dates OWNER TO doadmin;

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
    rp.unique_id AS rp_unique_id
   FROM (rp_raw.performance_data rp
     LEFT JOIN staging.joined_performance_data sjpd ON ((rp.unique_id = (sjpd.rp_unique_id)::text)))
  WHERE (sjpd.rp_unique_id IS NULL);

ALTER VIEW staging.missing_performance_data OWNER TO doadmin;

CREATE VIEW tf_raw.all_entities AS
 WITH rp_raw_entities AS (
         SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
            regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
                CASE
                    WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
                    ELSE NULL::numeric
                END AS converted_finishing_position,
            pd.sire_name,
            regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
            pd.sire_id,
            pd.dam_name,
            regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
            pd.dam_id,
            pd.race_timestamp,
            c.course_id,
            pd.unique_id,
            (pd.race_timestamp)::date AS race_date
           FROM (rp_raw.performance_data pd
             LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
        ), tf_raw_entities AS (
         SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
            regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
                CASE
                    WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
                    ELSE NULL::numeric
                END AS converted_finishing_position,
            pd.sire_name,
            regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
            pd.sire_id,
            pd.dam_name,
            regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
            pd.dam_id,
            pd.race_timestamp,
            c.course_id,
            pd.unique_id,
            (pd.race_timestamp)::date AS race_date
           FROM (tf_raw.performance_data pd
             LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)))
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
             LEFT JOIN tf_raw_entities tf ON (((rp.race_timestamp = tf.race_timestamp) AND ((rp.course_id)::text = (tf.course_id)::text) AND (rp.converted_finishing_position = tf.converted_finishing_position) AND (rp.horse_age = tf.horse_age))))
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

CREATE VIEW tf_raw.formatted_tf_entities AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_name,
    regexp_replace(lower(regexp_replace(pd.horse_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_horse_name,
    pd.horse_id,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    regexp_replace(lower(regexp_replace(pd.jockey_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    regexp_replace(lower(regexp_replace(pd.trainer_name, '\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_trainer_name,
        CASE
            WHEN (pd.finishing_position ~ '^[0-9]+(\.[0-9]+)?$'::text) THEN (pd.finishing_position)::numeric
            ELSE NULL::numeric
        END AS converted_finishing_position,
    pd.sire_name,
    regexp_replace(lower(regexp_replace(pd.sire_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_sire_name,
    pd.sire_id,
    pd.dam_name,
    regexp_replace(lower(regexp_replace(pd.dam_name, '\s*\([^)]*\)'::text, ''::text, 'g'::text)), '\s+'::text, ''::text, 'g'::text) AS filtered_dam_name,
    pd.dam_id,
    pd.race_timestamp,
    c.course_id,
    pd.unique_id,
    (pd.race_timestamp)::date AS race_date
   FROM (tf_raw.performance_data pd
     LEFT JOIN public.course c ON ((pd.course_id = (c.tf_course_id)::text)));

ALTER VIEW tf_raw.formatted_tf_entities OWNER TO doadmin;

CREATE VIEW tf_raw.missing_dates AS
 SELECT gs.date
   FROM (generate_series(('2005-01-01'::date)::timestamp without time zone, ((now())::date - '3 days'::interval), '1 day'::interval) gs(date)
     LEFT JOIN ( SELECT DISTINCT days_results_links.date
           FROM tf_raw.days_results_links) distinct_dates ON ((gs.date = distinct_dates.date)))
  WHERE ((distinct_dates.date IS NULL) AND (NOT (gs.date = ANY (ARRAY[('2005-03-25'::date)::timestamp without time zone, ('2009-12-23'::date)::timestamp without time zone, ('2020-04-06'::date)::timestamp without time zone, ('2008-12-25'::date)::timestamp without time zone, ('2013-03-29'::date)::timestamp without time zone, ('2010-01-06'::date)::timestamp without time zone, ('2008-12-24'::date)::timestamp without time zone, ('2009-02-02'::date)::timestamp without time zone, ('2007-12-25'::date)::timestamp without time zone, ('2007-12-24'::date)::timestamp without time zone, ('2005-12-25'::date)::timestamp without time zone, ('2005-12-24'::date)::timestamp without time zone, ('2009-12-25'::date)::timestamp without time zone, ('2009-12-24'::date)::timestamp without time zone, ('2006-12-25'::date)::timestamp without time zone, ('2006-12-24'::date)::timestamp without time zone, ('2020-04-03'::date)::timestamp without time zone, ('2020-04-02'::date)::timestamp without time zone, ('2015-12-25'::date)::timestamp without time zone, ('2015-12-24'::date)::timestamp without time zone, ('2016-12-25'::date)::timestamp without time zone, ('2016-12-24'::date)::timestamp without time zone, ('2017-12-25'::date)::timestamp without time zone, ('2017-12-24'::date)::timestamp without time zone, ('2018-12-25'::date)::timestamp without time zone, ('2018-12-24'::date)::timestamp without time zone, ('2019-12-25'::date)::timestamp without time zone, ('2019-12-24'::date)::timestamp without time zone, ('2020-12-25'::date)::timestamp without time zone, ('2020-12-24'::date)::timestamp without time zone, ('2021-12-25'::date)::timestamp without time zone, ('2021-12-24'::date)::timestamp without time zone, ('2022-12-25'::date)::timestamp without time zone, ('2022-12-24'::date)::timestamp without time zone, ('2023-12-25'::date)::timestamp without time zone, ('2023-12-24'::date)::timestamp without time zone]))))
  ORDER BY gs.date DESC;

ALTER VIEW tf_raw.missing_dates OWNER TO doadmin;

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

