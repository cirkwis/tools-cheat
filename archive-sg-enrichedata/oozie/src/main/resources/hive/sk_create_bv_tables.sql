DROP TABLE IF EXISTS ${hivevar:cbsBase}.sk_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.sk_le_bv(
    city string,
    nace_code string,
    dest_country_code string,
    headquarters_address string,
    headquarters_address_code_postal string,
    legal_name_90c string,
    legal_form string,
    other_national_id string,
    annual_sales struct<local_range_low:string,local_range_high:string,local_range_currency:string,range_year:string>,
    employees struct<total_range_low:string,total_range_high:string,total_range_year:string>,
    nace_rev_2 struct<section:string,division:string,groupe:string,classe:string,sous_classe:string>,
    duplicates_count bigint
    )
PARTITIONED BY (yyyy STRING, mm STRING, dd String)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/sk_le_bv';