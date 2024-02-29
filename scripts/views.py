import sys
from src.storage.sql_db import execute_query

drop_views = """
DROP VIEW IF EXISTS tf_raw.missing_trainers;
DROP VIEW IF EXISTS tf_raw.missing_sires;
DROP VIEW IF EXISTS tf_raw.missing_jockeys;
DROP VIEW IF EXISTS tf_raw.missing_horses;


DROP VIEW IF EXISTS rp_raw.missing_horses;
DROP VIEW IF EXISTS rp_raw.missing_jockeys;
DROP VIEW IF EXISTS rp_raw.missing_sires;
DROP VIEW IF EXISTS rp_raw.missing_trainers;

"""
create_views = """
CREATE OR REPLACE VIEW tf_raw.missing_trainers
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.sire_id,
    pd.sire_name,
    pd.dam_id,
    pd.dam_name,
    pd.finishing_position,
    pd.horse_name_link,
    pd.course,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.race_id,
    pd.debug_link,
    pd.unique_id
   FROM tf_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.tf_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.tf_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.tf_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.tf_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.tf_sire_id::text
  WHERE t.trainer_id IS NULL;

ALTER TABLE tf_raw.missing_trainers
    OWNER TO doadmin;
	

CREATE OR REPLACE VIEW tf_raw.missing_sires
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.sire_id,
    pd.sire_name,
    pd.dam_id,
    pd.dam_name,
    pd.finishing_position,
    pd.horse_name_link,
    pd.course,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.race_id,
    pd.debug_link,
    pd.unique_id
   FROM tf_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.tf_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.tf_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.tf_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.tf_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.tf_sire_id::text
  WHERE s.sire_id IS NULL;

ALTER TABLE tf_raw.missing_sires
    OWNER TO doadmin;
	

CREATE OR REPLACE VIEW tf_raw.missing_jockeys
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.sire_id,
    pd.sire_name,
    pd.dam_id,
    pd.dam_name,
    pd.finishing_position,
    pd.horse_name_link,
    pd.course,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.race_id,
    pd.debug_link,
    pd.unique_id
   FROM tf_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.tf_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.tf_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.tf_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.tf_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.tf_sire_id::text
  WHERE j.jockey_id IS NULL;

ALTER TABLE tf_raw.missing_jockeys
    OWNER TO doadmin;
	
	
-- View: tf_raw.missing_horses



CREATE OR REPLACE VIEW tf_raw.missing_horses
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.sire_id,
    pd.sire_name,
    pd.dam_id,
    pd.dam_name,
    pd.finishing_position,
    pd.horse_name_link,
    pd.course,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.race_id,
    pd.debug_link,
    pd.unique_id
   FROM tf_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.tf_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.tf_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.tf_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.tf_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.tf_sire_id::text
  WHERE h.horse_id IS NULL;

ALTER TABLE tf_raw.missing_horses
    OWNER TO doadmin;

CREATE OR REPLACE VIEW rp_raw.missing_horses
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.owner_id,
    pd.owner_name,
    pd.finishing_position,
    pd.sire,
    pd.sire_id,
    pd.dam,
    pd.dam_id,
    pd.dams_sire,
    pd.dams_sire_id,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.course_name,
    pd.course,
    pd.debug_link,
    pd.race_id,
    pd.unique_id
   FROM rp_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.rp_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.rp_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.rp_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.rp_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.rp_sire_id::text
     LEFT JOIN dam d ON pd.dam_id = d.rp_dam_id::text
  WHERE h.rp_horse_id IS NULL;

ALTER TABLE rp_raw.missing_horses
    OWNER TO doadmin;

  CREATE OR REPLACE VIEW rp_raw.missing_jockeys
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.owner_id,
    pd.owner_name,
    pd.finishing_position,
    pd.sire,
    pd.sire_id,
    pd.dam,
    pd.dam_id,
    pd.dams_sire,
    pd.dams_sire_id,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.course_name,
    pd.course,
    pd.debug_link,
    pd.race_id,
    pd.unique_id
   FROM rp_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.rp_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.rp_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.rp_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.rp_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.rp_sire_id::text
     LEFT JOIN dam d ON pd.dam_id = d.rp_dam_id::text
  WHERE j.rp_jockey_id IS NULL;

ALTER TABLE rp_raw.missing_jockeys
    OWNER TO doadmin;

CREATE OR REPLACE VIEW rp_raw.missing_sires
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.owner_id,
    pd.owner_name,
    pd.finishing_position,
    pd.sire,
    pd.sire_id,
    pd.dam,
    pd.dam_id,
    pd.dams_sire,
    pd.dams_sire_id,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.course_name,
    pd.course,
    pd.debug_link,
    pd.race_id,
    pd.unique_id
   FROM rp_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.rp_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.rp_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.rp_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.rp_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.rp_sire_id::text
     LEFT JOIN dam d ON pd.dam_id = d.rp_dam_id::text
  WHERE s.rp_sire_id IS NULL;

ALTER TABLE rp_raw.missing_sires
    OWNER TO doadmin;

CREATE OR REPLACE VIEW rp_raw.missing_trainers
 AS
 SELECT DISTINCT ON (pd.unique_id) pd.horse_id,
    pd.horse_name,
    pd.horse_age,
    pd.jockey_id,
    pd.jockey_name,
    pd.trainer_id,
    pd.trainer_name,
    pd.owner_id,
    pd.owner_name,
    pd.finishing_position,
    pd.sire,
    pd.sire_id,
    pd.dam,
    pd.dam_id,
    pd.dams_sire,
    pd.dams_sire_id,
    pd.race_date,
    pd.race_time,
    pd.race_timestamp,
    c.course_id,
    pd.course_name,
    pd.course,
    pd.debug_link,
    pd.race_id,
    pd.unique_id
   FROM rp_raw.performance_data pd
     LEFT JOIN horse h ON pd.horse_id = h.rp_horse_id::text
     LEFT JOIN course c ON pd.course_id = c.rp_course_id::text
     LEFT JOIN jockey j ON pd.jockey_id = j.rp_jockey_id::text
     LEFT JOIN trainer t ON pd.trainer_id = t.rp_trainer_id::text
     LEFT JOIN sire s ON pd.sire_id = s.rp_sire_id::text
     LEFT JOIN dam d ON pd.dam_id = d.rp_dam_id::text
  WHERE t.rp_trainer_id IS NULL;

ALTER TABLE rp_raw.missing_trainers
    OWNER TO doadmin;



"""
def main(args):
    if args[0] == 'drop':
        execute_query(drop_views)
    elif args[0] == 'create':
        execute_query(create_views)

if __name__ == '__main__':
    main(sys.argv[1:])
