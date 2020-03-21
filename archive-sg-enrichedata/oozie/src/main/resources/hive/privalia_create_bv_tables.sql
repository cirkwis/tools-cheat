DROP TABLE IF EXISTS ${hivevar:cbsBase}.privalia_pm_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.privalia_pm_bv(
    client_type string,
    compliance_status string,
    customer_segment string,
    date_contact string,
    engagements_level string,
    global_revenu string,
    identifier string,
    mif_profil_code string,
    mif_profil_name string,
    patrimoine_level string,
    top_ppe string,
    ppe_code string,
    ppe_name string,
    privileged_phone string,
    rct_identifier string,
    relationship_end_date string,
    relationship_start_date string,
    relationship_type_status string,
    risk_code string,
    risk_label string,
    secteur_activite_code string,
    secteur_activite_name string,
    top_conformity_eer string,
    top_risk_complete string,
    topgip string,
    total_aum string,
    type_contact_code string,
    type_contact_name string,
    pp array<struct<code_nation:string,code_pays_nais:string,code_pays_rfis:string,code_pays_rmon:string,first_name:string,fonction_interv:string,identifier:string,last_name:string,pourc_detention:string,authority_other:string,authority_account:string,authority_securities:string,ref_procuration:string,sous_type_indiv:string,type_de_lien:string,type_de_lien_economique:string,nature_droits:string,statut_tiers_pp:string,type_tiers_pp:string>>, ccl array<struct<date_creation_cercle:string,num_cercle:string,role_tiers_cercle_code:string,role_tiers_cercle_name:string,top_referent:string,type_cercle:string>>,
    nace_rev_2_detail struct<section:string,division:string,groupe:string,classe:string,sous_classe:string>,
    duplicates_count bigint)
PARTITIONED BY (yyyy string,mm string,dd String)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/privalia_pm_bv';




DROP TABLE IF EXISTS ${hivevar:cbsBase}.privalia_pp_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.privalia_pp_bv (
client_type string,
compliance_status string,
customer_segment string,
date_contact string,
engagements_level string,
global_revenu string,
identifier string,
mif_profil_code string,
mif_profil_name string,
mif_skill string,
patrimoine_level string,
phone_mobile string,
politically_exposed string,
ppe_code string,
ppe_name string,
relationship_end_date string,
relationship_start_date string,
relationship_type_status string,
risk_code string,
top_risk_complete string,
risk_label string,
top_conformity_eer string,
topgip string,
total_aum string,
type_contact_code string,
type_contact_name string,
pm array<struct<identifier:string,legal_name_90c:string,type_de_lien:string,type_de_lien_economique:string,nature_droits:string,pourc_detention:string,rct_identifier:string,statut_tiers_pm:string,type_tiers_pm:string>>,
ccl array<struct<date_creation_cercle:string,num_cercle:string,role_tiers_cercle_code:string,role_tiers_cercle_name:string,top_referent:string,type_cercle:string>>,
nace_rev_2_detail struct<section:string,division:string,groupe:string,classe:string,sous_classe:string>,
duplicates_count bigint)
PARTITIONED BY (yyyy STRING, mm STRING, dd String)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/privalia_pp_bv';