CREATE VIEW metrics.missing_entity_counts_vw AS
 WITH unordered AS (
         SELECT count(*) AS count,
            'rp_unmatched_dams'::text AS missing_entity_type
           FROM metrics.rp_unmatched_dams
        UNION
         SELECT count(*) AS count,
            'rp_unmatched_sires'::text AS missing_entity_type
           FROM metrics.rp_unmatched_sires
        UNION
         SELECT count(*) AS count,
            'rp_unmatched_horses'::text AS missing_entity_type
           FROM metrics.rp_unmatched_horses
        UNION
         SELECT count(*) AS count,
            'rp_unmatched_trainers'::text AS missing_entity_type
           FROM metrics.rp_unmatched_trainers
        UNION
         SELECT count(*) AS count,
            'rp_unmatched_jockeys'::text AS missing_entity_type
           FROM metrics.rp_unmatched_jockeys
        UNION
         SELECT count(*) AS count,
            'tf_unmatched_dams'::text AS missing_entity_type
           FROM metrics.tf_unmatched_dams
        UNION
         SELECT count(*) AS count,
            'tf_unmatched_sires'::text AS missing_entity_type
           FROM metrics.tf_unmatched_sires
        UNION
         SELECT count(*) AS count,
            'tf_unmatched_horses'::text AS missing_entity_type
           FROM metrics.tf_unmatched_horses
        UNION
         SELECT count(*) AS count,
            'tf_unmatched_trainers'::text AS missing_entity_type
           FROM metrics.tf_unmatched_trainers
        UNION
         SELECT count(*) AS count,
            'tf_unmatched_jockeys'::text AS missing_entity_type
           FROM metrics.tf_unmatched_jockeys
        )
 SELECT count,
    missing_entity_type
   FROM unordered
  ORDER BY count DESC;

ALTER VIEW metrics.missing_entity_counts_vw OWNER TO postgres;

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

CREATE VIEW metrics.record_count_differences_gt_2010_vw AS
 SELECT course,
    race_date,
    rp_num_records,
    tf_num_records
   FROM metrics.record_count_differences_vw
  WHERE ((race_date)::date > '2010-01-01'::date);

ALTER VIEW metrics.record_count_differences_gt_2010_vw OWNER TO doadmin;

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
    spd.created_at
   FROM (staging.joined_performance_data spd
     LEFT JOIN public.performance_data pd ON (((spd.unique_id)::text = (pd.unique_id)::text)))
  WHERE ((pd.unique_id IS NULL) AND ((spd.race_date)::text > '2010-01-01'::text));

ALTER VIEW public.missing_performance_data_vw OWNER TO doadmin;

CREATE VIEW public.missing_race_data_vw AS
 SELECT spd.race_time,
    spd.race_date,
    spd.race_title,
    spd.race_type,
    spd.race_class,
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
    spd.total_prize_money,
    spd.first_place_prize_money,
    spd.winning_time,
    spd.time_seconds,
    spd.relative_time,
    spd.relative_to_standard,
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

CREATE VIEW rp_raw.base_formatted_entities AS
 WITH historical_data AS (
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
        ), present_data AS (
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
           FROM (rp_raw.todays_performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.rp_id)::text)))
        )
 SELECT historical_data.horse_name,
    historical_data.filtered_horse_name,
    historical_data.horse_id,
    historical_data.horse_age,
    historical_data.jockey_id,
    historical_data.jockey_name,
    historical_data.filtered_jockey_name,
    historical_data.trainer_id,
    historical_data.trainer_name,
    historical_data.filtered_trainer_name,
    historical_data.converted_finishing_position,
    historical_data.sire_name,
    historical_data.filtered_sire_name,
    historical_data.sire_id,
    historical_data.dam_name,
    historical_data.filtered_dam_name,
    historical_data.dam_id,
    historical_data.race_timestamp,
    historical_data.course_id,
    historical_data.unique_id,
    historical_data.race_date
   FROM historical_data
UNION
 SELECT present_data.horse_name,
    present_data.filtered_horse_name,
    present_data.horse_id,
    present_data.horse_age,
    present_data.jockey_id,
    present_data.jockey_name,
    present_data.filtered_jockey_name,
    present_data.trainer_id,
    present_data.trainer_name,
    present_data.filtered_trainer_name,
    present_data.converted_finishing_position,
    present_data.sire_name,
    present_data.filtered_sire_name,
    present_data.sire_id,
    present_data.dam_name,
    present_data.filtered_dam_name,
    present_data.dam_id,
    present_data.race_timestamp,
    present_data.course_id,
    present_data.unique_id,
    present_data.race_date
   FROM present_data;

ALTER VIEW rp_raw.base_formatted_entities OWNER TO doadmin;

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
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2010-01-01'::date) AND (cr.course_part = ANY (ARRAY['aintree'::text, 'ascot'::text, 'ayr'::text, 'ballinrobe'::text, 'bangor-on-dee'::text, 'bath'::text, 'bellewstown'::text, 'beverley'::text, 'brighton'::text, 'carlisle'::text, 'cartmel'::text, 'catterick'::text, 'chelmsford-aw'::text, 'cheltenham'::text, 'chepstow'::text, 'chester'::text, 'clonmel'::text, 'cork'::text, 'curragh'::text, 'doncaster'::text, 'down-royal'::text, 'downpatrick'::text, 'dundalk-aw'::text, 'epsom'::text, 'exeter'::text, 'fairyhouse'::text, 'fakenham'::text, 'ffos-las'::text, 'fontwell'::text, 'galway'::text, 'goodwood'::text, 'gowran-park'::text, 'hamilton'::text, 'haydock'::text, 'hereford'::text, 'hexham'::text, 'huntingdon'::text, 'kelso'::text, 'kempton'::text, 'kempton-aw'::text, 'kilbeggan'::text, 'killarney'::text, 'laytown'::text, 'leicester'::text, 'leopardstown'::text, 'limerick'::text, 'lingfield'::text, 'lingfield-aw'::text, 'listowel'::text, 'ludlow'::text, 'market-rasen'::text, 'musselburgh'::text, 'naas'::text, 'navan'::text, 'newbury'::text, 'newcastle'::text, 'newcastle-aw'::text, 'newmarket'::text, 'newmarket-july'::text, 'newton-abbot'::text, 'nottingham'::text, 'perth'::text, 'plumpton'::text, 'pontefract'::text, 'punchestown'::text, 'redcar'::text, 'ripon'::text, 'roscommon'::text, 'salisbury'::text, 'sandown'::text, 'sedgefield'::text, 'sligo'::text, 'southwell'::text, 'southwell-aw'::text, 'stratford'::text, 'taunton'::text, 'thirsk'::text, 'thurles'::text, 'tipperary'::text, 'towcester'::text, 'tramore'::text, 'uttoxeter'::text, 'warwick'::text, 'wetherby'::text, 'wexford'::text, 'wexford-rh'::text, 'wincanton'::text, 'windsor'::text, 'wolverhampton-aw'::text, 'worcester'::text, 'yarmouth'::text, 'york'::text])) AND (cr.link <> ALL (ARRAY['https://www.racingpost.com/results/87/wetherby/2011-10-12/539792'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2007-12-10/444939'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/101/worcester/2023-05-26/839708'::text, 'https://www.racingpost.com/results/15/doncaster/2009-12-12/494914'::text, 'https://www.racingpost.com/results/83/towcester/2011-03-17/525075'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2022-03-04/804298'::text, 'https://www.racingpost.com/results/25/hexham/2021-05-01/781541'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2010-10-03/515581'::text, 'https://www.racingpost.com/results/17/epsom/2014-08-25/608198'::text, 'https://www.racingpost.com/results/57/sedgefield/2014-11-25/613650'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2017-02-23/668139'::text, 'https://www.racingpost.com/results/41/perth/2017-09-11/682957'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2020-03-20/754104'::text, 'https://www.racingpost.com/results/20/fontwell/2016-09-30/658390'::text, 'https://www.racingpost.com/results/9/cartmel/2018-05-30/701255'::text, 'https://www.racingpost.com/results/34/ludlow/2014-05-11/600195'::text, 'https://www.racingpost.com/results/8/carlisle/2011-02-21/523611'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/90/wincanton/2016-12-26/664813'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/44/plumpton/2021-03-01/777465'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2018-11-13/714479'::text, 'https://www.racingpost.com/results/54/sandown/2019-12-07/744492'::text, 'https://www.racingpost.com/results/4/bangor-on-dee/2022-10-25/822558'::text, 'https://www.racingpost.com/results/67/stratford/2023-06-20/842207'::text, 'https://www.racingpost.com/results/23/haydock/2018-12-30/717697'::text, 'https://www.racingpost.com/results/44/plumpton/2022-04-18/807343'::text, 'https://www.racingpost.com/results/37/newcastle/2024-03-05/860189'::text])));

ALTER VIEW rp_raw.missing_links OWNER TO doadmin;

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
    rp.official_rating,
    rp.finishing_position,
    rp.total_distance_beaten,
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
    rp.race_class,
    rp.conditions,
    rp.distance,
    rp.distance_full,
    rp.going,
    rp.winning_time,
    rp.horse_type,
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

CREATE VIEW tf_raw.base_formatted_entities AS
 WITH historical_data AS (
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
          WHERE (pd.race_timestamp > (now() - '5 years'::interval))
        ), present_data AS (
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
           FROM (tf_raw.todays_performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_id)::text)))
        )
 SELECT historical_data.horse_name,
    historical_data.filtered_horse_name,
    historical_data.horse_id,
    historical_data.horse_age,
    historical_data.jockey_id,
    historical_data.jockey_name,
    historical_data.filtered_jockey_name,
    historical_data.trainer_id,
    historical_data.trainer_name,
    historical_data.filtered_trainer_name,
    historical_data.converted_finishing_position,
    historical_data.sire_name,
    historical_data.filtered_sire_name,
    historical_data.sire_id,
    historical_data.dam_name,
    historical_data.filtered_dam_name,
    historical_data.dam_id,
    historical_data.race_timestamp,
    historical_data.course_id,
    historical_data.unique_id,
    historical_data.race_date
   FROM historical_data
UNION
 SELECT present_data.horse_name,
    present_data.filtered_horse_name,
    present_data.horse_id,
    present_data.horse_age,
    present_data.jockey_id,
    present_data.jockey_name,
    present_data.filtered_jockey_name,
    present_data.trainer_id,
    present_data.trainer_name,
    present_data.filtered_trainer_name,
    present_data.converted_finishing_position,
    present_data.sire_name,
    present_data.filtered_sire_name,
    present_data.sire_id,
    present_data.dam_name,
    present_data.filtered_dam_name,
    present_data.dam_id,
    present_data.race_timestamp,
    present_data.course_id,
    present_data.unique_id,
    present_data.race_date
   FROM present_data;

ALTER VIEW tf_raw.base_formatted_entities OWNER TO doadmin;

CREATE VIEW tf_raw.formatted_tf_entities AS
 WITH historical_data AS (
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
          WHERE (pd.race_timestamp > (now() - '5 years'::interval))
        ), present_data AS (
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
           FROM (tf_raw.todays_performance_data pd
             LEFT JOIN public.course c ON (((pd.course_id)::text = (c.tf_id)::text)))
        )
 SELECT historical_data.horse_name,
    historical_data.filtered_horse_name,
    historical_data.horse_id,
    historical_data.horse_age,
    historical_data.jockey_id,
    historical_data.jockey_name,
    historical_data.filtered_jockey_name,
    historical_data.trainer_id,
    historical_data.trainer_name,
    historical_data.filtered_trainer_name,
    historical_data.converted_finishing_position,
    historical_data.sire_name,
    historical_data.filtered_sire_name,
    historical_data.sire_id,
    historical_data.dam_name,
    historical_data.filtered_dam_name,
    historical_data.dam_id,
    historical_data.race_timestamp,
    historical_data.course_id,
    historical_data.unique_id,
    historical_data.race_date
   FROM historical_data
UNION
 SELECT present_data.horse_name,
    present_data.filtered_horse_name,
    present_data.horse_id,
    present_data.horse_age,
    present_data.jockey_id,
    present_data.jockey_name,
    present_data.filtered_jockey_name,
    present_data.trainer_id,
    present_data.trainer_name,
    present_data.filtered_trainer_name,
    present_data.converted_finishing_position,
    present_data.sire_name,
    present_data.filtered_sire_name,
    present_data.sire_id,
    present_data.dam_name,
    present_data.filtered_dam_name,
    present_data.dam_id,
    present_data.race_timestamp,
    present_data.course_id,
    present_data.unique_id,
    present_data.race_date
   FROM present_data;

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

