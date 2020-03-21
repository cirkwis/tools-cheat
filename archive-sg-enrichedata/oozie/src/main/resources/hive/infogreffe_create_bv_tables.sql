DROP TABLE IF EXISTS ${hivevar:cbsBase}.infogreffe_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.infogreffe_le_bv
  (siret STRING, ape_code STRING, ape_name STRING, business_listing STRING, city STRING, company_creation_date STRING, legal_form STRING, legal_form_code STRING, legal_name_90c STRING, region STRING, registry_name STRING, headquarters_address STRING, deregistration_date STRING, as_2011 STRING, as_2012 STRING, as_2013 STRING, as_2014 STRING, as_2015 STRING, as_2016 STRING, as_bracket_2011 STRING, as_bracket_2012 STRING, as_bracket_2013 STRING, as_bracket_2014 STRING, as_bracket_2015 STRING, as_bracket_2016 STRING, profit_2011 STRING, profit_2012 STRING, profit_2013 STRING, profit_2014 STRING, profit_2015 STRING, profit_2016 STRING, workforce_2011 STRING, workforce_2012 STRING, workforce_2013 STRING, workforce_2014 STRING, workforce_2015 STRING, workforce_2016 STRING, siren STRING, registry_id STRING, registry_local_site STRING, legal_situation STRING, hash_id STRING)
PARTITIONED BY (yyyy STRING, mm STRING,dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/infogreffe_le_bv';
