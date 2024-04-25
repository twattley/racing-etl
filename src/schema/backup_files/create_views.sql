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
  WHERE ((rp.num_records <> tf.num_records) OR (rp.num_records IS NULL) OR ((tf.num_records IS NULL) AND ((rp.race_date)::date > '2010-01-01'::date)))
  ORDER BY COALESCE(rp.race_date, tf.race_date) DESC, COALESCE(rp.course_name, tf.course_name), rp.course_id;

ALTER VIEW metrics.record_count_differences_vw OWNER TO doadmin;

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
  WHERE ((pd.debug_link IS NULL) AND (cr.date > '2010-01-01'::date) AND (cr.course_part = ANY (ARRAY['aintree'::text, 'ascot'::text, 'ayr'::text, 'ballinrobe'::text, 'bangor-on-dee'::text, 'bath'::text, 'bellewstown'::text, 'beverley'::text, 'brighton'::text, 'carlisle'::text, 'cartmel'::text, 'catterick'::text, 'chelmsford-aw'::text, 'cheltenham'::text, 'chepstow'::text, 'chester'::text, 'clonmel'::text, 'cork'::text, 'curragh'::text, 'doncaster'::text, 'down-royal'::text, 'downpatrick'::text, 'dundalk-aw'::text, 'epsom'::text, 'exeter'::text, 'fairyhouse'::text, 'fakenham'::text, 'ffos-las'::text, 'fontwell'::text, 'galway'::text, 'goodwood'::text, 'gowran-park'::text, 'hamilton'::text, 'haydock'::text, 'hereford'::text, 'hexham'::text, 'huntingdon'::text, 'kelso'::text, 'kempton'::text, 'kempton-aw'::text, 'kilbeggan'::text, 'killarney'::text, 'laytown'::text, 'leicester'::text, 'leopardstown'::text, 'limerick'::text, 'lingfield'::text, 'lingfield-aw'::text, 'listowel'::text, 'ludlow'::text, 'market-rasen'::text, 'musselburgh'::text, 'naas'::text, 'navan'::text, 'newbury'::text, 'newcastle'::text, 'newcastle-aw'::text, 'newmarket'::text, 'newmarket-july'::text, 'newton-abbot'::text, 'nottingham'::text, 'perth'::text, 'plumpton'::text, 'pontefract'::text, 'punchestown'::text, 'redcar'::text, 'ripon'::text, 'roscommon'::text, 'salisbury'::text, 'sandown'::text, 'sedgefield'::text, 'sligo'::text, 'southwell'::text, 'southwell-aw'::text, 'stratford'::text, 'taunton'::text, 'thirsk'::text, 'thurles'::text, 'tipperary'::text, 'towcester'::text, 'tramore'::text, 'uttoxeter'::text, 'warwick'::text, 'wetherby'::text, 'wexford'::text, 'wexford-rh'::text, 'wincanton'::text, 'windsor'::text, 'wolverhampton-aw'::text, 'worcester'::text, 'yarmouth'::text, 'york'::text])) AND (cr.link <> ALL (ARRAY['https://www.racingpost.com/results/87/wetherby/2011-10-12/539792'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2007-12-10/444939'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/101/worcester/2023-05-26/839708'::text, 'https://www.racingpost.com/results/15/doncaster/2009-12-12/494914'::text, 'https://www.racingpost.com/results/83/towcester/2011-03-17/525075'::text, 'https://www.racingpost.com/results/16/musselburgh/2019-11-06/742103'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2022-03-04/804298'::text, 'https://www.racingpost.com/results/25/hexham/2021-05-01/781541'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2010-10-03/515581'::text, 'https://www.racingpost.com/results/17/epsom/2014-08-25/608198'::text, 'https://www.racingpost.com/results/57/sedgefield/2014-11-25/613650'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2017-02-23/668139'::text, 'https://www.racingpost.com/results/41/perth/2017-09-11/682957'::text, 'https://www.racingpost.com/results/1138/dundalk-aw/2020-03-20/754104'::text, 'https://www.racingpost.com/results/20/fontwell/2016-09-30/658390'::text, 'https://www.racingpost.com/results/9/cartmel/2018-05-30/701255'::text, 'https://www.racingpost.com/results/34/ludlow/2014-05-11/600195'::text, 'https://www.racingpost.com/results/8/carlisle/2011-02-21/523611'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/513/wolverhampton-aw/2023-07-11/843383'::text, 'https://www.racingpost.com/results/90/wincanton/2016-12-26/664813'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2023-10-19/849854'::text, 'https://www.racingpost.com/results/393/lingfield-aw/2013-06-22/580503'::text, 'https://www.racingpost.com/results/44/plumpton/2021-03-01/777465'::text, 'https://www.racingpost.com/results/1083/chelmsford-aw/2018-11-13/714479'::text, 'https://www.racingpost.com/results/54/sandown/2019-12-07/744492'::text, 'https://www.racingpost.com/results/4/bangor-on-dee/2022-10-25/822558'::text, 'https://www.racingpost.com/results/67/stratford/2023-06-20/842207'::text, 'https://www.racingpost.com/results/23/haydock/2018-12-30/717697'::text, 'https://www.racingpost.com/results/44/plumpton/2022-04-18/807343'::text, 'https://www.racingpost.com/results/37/newcastle/2024-03-05/860189'::text, 'https://www.racingpost.com/results/27/kelso/2024-04-15/863253'::text])));

ALTER VIEW rp_raw.missing_links OWNER TO doadmin;

CREATE VIEW rp_raw.todays_races AS
 SELECT DISTINCT race_date,
    course,
    race_timestamp
   FROM rp_raw.todays_performance_data
  WHERE ((race_date)::date >= CURRENT_DATE)
  ORDER BY race_date, course, race_timestamp;

ALTER VIEW rp_raw.todays_races OWNER TO postgres;

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

