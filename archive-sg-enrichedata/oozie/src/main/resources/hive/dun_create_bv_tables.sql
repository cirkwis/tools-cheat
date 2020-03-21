DROP TABLE IF EXISTS ${hivevar:cbsBase}.dun_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.dun_le_bv(
acronym                 STRING,
address_du              STRING,
address_gu              STRING,
address_hq              STRING,
business_name_du        STRING,
business_name_gu        STRING,
business_name_hq        STRING,
chief_executive_officer_name    STRING,
chief_executive_officer_title   STRING,
city                    STRING,
continent               STRING,
creation_date           STRING,
currency_code           STRING,
dest_country            STRING,
dest_country_du         STRING,
dest_country_gu         STRING,
dest_country_hq         STRING,
dun_id                  STRING,
duns_number_du          STRING,
duns_number_gu          STRING,
duns_number_hq          STRING,
employees_here          STRING,
family_members_number_gu        STRING,
family_update_date      STRING,
group_number_legal_entities     STRING,
legal_form              STRING,
legal_name_90c          STRING,
legal_situation_code    STRING,
local_annual_sales      STRING,
mailing_address         STRING,
naer5_code              STRING,
other_national_id       STRING,
physical_address        STRING,
registered_address_indicator    STRING,
report_date             STRING,
sic_cod_1987_2          STRING,
sic_cod_1987_3          STRING,
sic_cod_1987_4          STRING,
sic_cod_1987_5          STRING,
sic_cod_1987_6          STRING,
sic_code_1987           STRING,
siren                   STRING,
siret                   STRING,
status                  STRING,
subsidiary              STRING,
total_employees         STRING,
us_dol_annual_sales     STRING,
duplicates_count        BIGINT)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/dun_le_bv';