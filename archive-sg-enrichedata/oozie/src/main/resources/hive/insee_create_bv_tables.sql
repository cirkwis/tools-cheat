DROP TABLE IF EXISTS ${hivevar:cbsBase}.insee_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.insee_le_bv(
    insee_hash              STRING,
    siret                   STRING,
    headquarters_address    STRING,
    database_deletion_date  STRING,
    legal_situation         STRING,
    legal_category_code     STRING,
    acronym                 STRING,
    ape_insee5_code         STRING,
    legal_name              STRING,
    legal_name_90c          STRING,
    siren_identifier        STRING,
    dest_country            STRING,
    company_creation_date   STRING,
    office_type             STRING,
    brand                   STRING,
    establishment_activity  STRING,
    establishment_activity_nature   STRING,
    monoactivity_index      STRING,
    business_category       STRING,
    annual_sales_local      STRING,
    employees_here          STRING,
    employees_total         STRING)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/insee_le_bv';
