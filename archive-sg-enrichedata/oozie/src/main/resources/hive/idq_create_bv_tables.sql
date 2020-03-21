DROP TABLE IF EXISTS ${hivevar:cbsBase}.idq_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.idq_le_bv
  (identifier String, legal_name_90c String, naer5_code String, nationality_country_code String, siren_identifier String, headquarters_address String, relationship_status String, duplicate_id String, duplicates_number String)
PARTITIONED BY (yyyy STRING, mm STRING, dd String)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/idq_le_bv';

