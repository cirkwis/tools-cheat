DROP TABLE IF EXISTS ${hivevar:cbsBase}.lei_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.lei_le_bv(
    city                    STRING,
    dest_country_code       STRING,
    headquarters_address    STRING,
    legal_address           STRING,
    legal_form              STRING,
    legal_name_90c          STRING,
    legal_situation         STRING,
    lei_id                  STRING,
    lei_lou_id              STRING,
    lei_status              STRING,
    other_national_id       STRING,
    relationship_status     STRING,
    siren                   STRING,
    duplicates_count        BIGINT)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/lei_le_bv';
