DROP TABLE IF EXISTS ${hivevar:cbsBase}.rct_idmut_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.rct_idmut_bv
  (idmut_le_identifier STRING, idmut_tymulti STRING, idmut_vamulti STRING)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/rct_idmut_bv';

DROP TABLE IF EXISTS ${hivevar:cbsBase}.rct_group_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.rct_group_bv
  (group_identifier STRING, group_name STRING, group_nationality_country STRING, group_nessg_code STRING, group_ultimate_parent_id STRING, group_number_legal_entities STRING, group_number_subgroups STRING)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/rct_group_bv';

DROP TABLE IF EXISTS ${hivevar:cbsBase}.rct_subgroup_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.rct_subgroup_bv
  (subgroup_identifier STRING, subgroup_name STRING, subgroup_nationality_country STRING, subgroup_nessg_code STRING, subgroup_leader_entity_id STRING, subgroup_number_legal_entities STRING)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/rct_subgroup_bv';

DROP TABLE IF EXISTS ${hivevar:cbsBase}.rct_network_relation_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.rct_network_relation_bv
  (network_acronym STRING, network_address STRING, network_client_type STRING, network_comm_denomination_90c STRING, network_commercial_denomination STRING, network_dest_country_code STRING, network_es_identifier STRING, network_flagprud STRING, network_generic_network_identifier STRING, network_immatriculation_entity STRING, network_le_identifier STRING, network_network_number STRING, network_relationship_status STRING, network_end_date STRING, network_relationship_type_status STRING, network_start_date STRING, network_statutory_third_parties_cat_code STRING, network_top_embargo STRING)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/rct_network_relation_bv';

DROP TABLE IF EXISTS ${hivevar:cbsBase}.rct_le_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.rct_le_bv
  (acquiring_le_identifier STRING, acronym STRING, activity_follow_up_code STRING, ape_insee5_code STRING, ape5_code STRING, big_regulatory_risk_code STRING, business_expiry_date STRING, client_type STRING, company_creation_date STRING, commercial_follow_up_code STRING, complement_dest_address STRING, delegated_pcru STRING, dest_country_code STRING, elr_code STRING, elr_legal_name STRING, elr_status STRING, end_relationship_reason_code STRING, flagprud STRING, headquarters_address STRING, identification_dest_address STRING, identifier STRING, legal_category_code STRING, legal_name STRING, legal_name_90c STRING, list_follow_up_code STRING, legal_situation_code STRING, naer5_code STRING, operational_center STRING, nessg_code STRING, out_business_date STRING, pcru_code STRING, payments_suspension_date STRING, principal_operating_entity_code STRING, profitability_study STRING, registration_entity STRING, post_office_address STRING, relationship_end_date STRING, relationship_start_date STRING, relationship_status STRING, relationship_type_status STRING, risk_unit STRING, siren_identifier STRING, statutory_third_parties_cat_code STRING, technical_follow_up STRING, top_embargo STRING, transverse_follow_up STRING, le_group_id STRING, le_subgroup_id STRING, is_parent_company STRING, scoring_is_parent_company STRING, headquarters_address_complement_geo STRING, headquarters_address_lieu_dit STRING, headquarters_address_numero_voie STRING, headquarters_address_code_postal STRING)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/rct_le_bv';

DROP TABLE IF EXISTS ${hivevar:cbsBase}.rct_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.rct_bv(
 acquiring_le_identifier STRING,
 acronym                 STRING,
 activity_follow_up_code STRING,
 ape_insee5_code         STRING,
 ape5_code               STRING,
 big_regulatory_risk_code        STRING,
 business_expiry_date    STRING,
 client_type             STRING,
 commercial_follow_up_code       STRING,
 complement_dest_address STRING,
 delegated_pcru          STRING,
 dest_country_code       STRING,
 dun_id                  STRING,
 elr_code                STRING,
 elr_legal_name          STRING,
 elr_status              STRING,
 end_relationship_reason_code    STRING,
 flagprud                STRING,
 group_number_legal_entities     STRING,
 group_identifier        STRING,
 group_name              STRING,
 group_nationality_country       STRING,
 group_nessg_code        STRING,
 group_number_subgroups  STRING,
 group_ultimate_parent_id        STRING,
 identification_dest_address     STRING,
 identifier              STRING,
 le_group_id             STRING,
 le_subgroup_id          STRING,
 legal_category_code     STRING,
 legal_name              STRING,
 legal_situation_code    STRING,
 lei_id                  STRING,
 list_follow_up_code     STRING,
 magnitude_id            STRING,
 naer5_code              STRING,
 nessg_code              STRING,
 network_relations       array<struct<network_acronym:STRING,network_address:STRING,network_client_type:STRING,network_comm_denomination_90c:STRING,network_commercial_denomination:STRING,network_dest_country_code:STRING,network_end_date:STRING,network_es_identifier:STRING,network_flagprud:STRING,network_generic_network_identifier:STRING,network_immatriculation_entity:STRING,network_network_number:STRING,network_relationship_status:STRING,network_relationship_type_status:STRING,network_start_date:STRING,network_statutory_third_parties_cat_code:STRING,network_top_embargo:STRING>>,
 operational_center      STRING,
 other_idmut             array<struct<idmut_tymulti:STRING,idmut_vamulti:STRING>>,
 out_business_date       STRING,
 payments_suspension_date        STRING,
 pcru_code               STRING,
 post_office_address     STRING,
 principal_operating_entity_code STRING,
 profitability_study     STRING,
 registration_entity     STRING,
 relationship_end_date   STRING,
 relationship_start_date STRING,
 relationship_status     STRING,
 relationship_type_status        STRING,
 risk_unit               STRING,
 statutory_third_parties_cat_code        STRING,
 stp_id                  STRING,
 subgroup_identifier     STRING,
 subgroup_leader_entity_id       STRING,
 subgroup_name           STRING,
 subgroup_nationality_country    STRING,
 subgroup_nessg_code     STRING,
 subgroup_number_legal_entities  STRING,
 swift_number            STRING,
 technical_follow_up     STRING,
 top_embargo             STRING,
 transverse_follow_up    STRING,
 company_creation_date   STRING,
 headquarters_address    STRING,
 legal_name_90c          STRING,
 siren_identifier        STRING,
 network_le_identifier   STRING,
 idmut_le_identifier     STRING,
 headquarters_address_complement_geo     STRING,
 headquarters_address_lieu_dit   STRING,
 headquarters_address_numero_voie        STRING,
 headquarters_address_code_postal        STRING,
 scoring_is_parent_company       STRING,
 is_parent_company      STRING,
 duplicates_count        BIGINT)
PARTITIONED BY (yyyy STRING , mm STRING ,dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/rct_bv';
