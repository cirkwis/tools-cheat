DROP TABLE IF EXISTS ${hivevar:cbsBase}.perle_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.perle_le_bv(
    acronym                 STRING,
    ape_insee5_code         STRING,
    business_line_supervision_pole  STRING,
    company_creation_date   STRING,
    date_of_dissolution     STRING,
    elr_code                STRING,
    financial_supervisor    STRING,
    legal_category_code     STRING,
    legal_name              STRING,
    legal_situation         STRING,
    lei_id                  STRING,
    magnitude_id            STRING,
    res_id                  STRING,
    siren_identifier        STRING,
    siret                   STRING,
    stp_id                  STRING,
    structure_type_id       STRING,
    supervision_department_id       STRING,
    supervision_sub_department_id   STRING,
    trade_as_name_1         STRING,
    archived                STRING,
    registry_name           STRING,
    elr_perimeter           STRING,
    city                    STRING,
    dest_country            STRING,
    headquarters_address    STRING,
    operational_address     STRING,
    physical_address        STRING,
    duplicates_count        BIGINT)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/perle_le_bv';