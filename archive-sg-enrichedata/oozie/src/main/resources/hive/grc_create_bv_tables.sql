DROP TABLE IF EXISTS ${hivevar:cbsBase}.grc_pp_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.grc_pp_bv(
    activity string,
    bad_id string,
    bddf_crs_status string,
    birth_country_code string,
    birth_date string,
    insee_birth_place string,
    authority_account string,
    client_type string,
    compliance_3c_top string,
    contract_sg_end_date string,
    contract_sg_start_date string,
    country_code string,
    csm_code string,
    currency_country_code string,
    current_type_ownership string,
    customer_incident string,
    death_date string,
    dec_code string,
    dec_name string,
    dom_top string,
    dr_code string,
    dr_name string,
    eer_client_status string,
    email string,
    employee_presence_top string,
    employer_address string,
    employer_country_code string,
    employer_name string,
    employer_psa_code string,
    end_relationship_reason_code string,
    end_relationship_reason_label string,
    family_situation string,
    fax string,
    first_name string,
    foreign_birth_place string,
    gender_code string,
    global_monthly_revenus string,
    address string,
    address_city string,
    address_code_postal string,
    address_numero_voie string,
    identifier string,
    inactive_client_top string,
    incident_nature string,
    ind_risk_lab string,
    ind_risk_lab_update_date string,
    interdict_bdf_code string,
    internet_connect_number string,
    internet_mail_top string,
    job_title string,
    last_name string,
    last_update_crs_status_monaco string,
    last_update_bddf_crs_statut_date string,
    legal_capacity_code string,
    living_deceased_code string,
    maiden_name string,
    main_agency string,
    matrimonial_regime string,
    monaco_crs_status string,
    nationality string,
    nsm_code string,
    number_of_children string,
    number_of_dependents string,
    authority_other string,
    pcs_code string,
    pcs_code_niv2 string,
    per_title string,
    phone_home string,
    phone_mobile string,
    phone_work string,
    politically_exposed string,
    privacy_indicator string,
    private_bank_top string,
    private_bank_top_update_date string,
    pro_status string,
    psa_code string,
    relationship_end_date string,
    relationship_start_date string,
    nature_of_client string,
    revenus string,
    risk_cotation_code string,
    risk_label string,
    risk_loss_relationship string,
    risk_loss_relationship_date string,
    authority_securities string,
    sg_personal_number string,
    sign_global_monthly_revenus string,
    sign_revenus string,
    tax_res_country_code string,
    tax_identifier string,
    top_expatriate string,
    transf_ceiling_bad string,
    customer_relationship_manager struct<agency:string,email:string,phone_number:string,last_name:string>,
    presta array<struct<address_type:string,amount_of_outstanding:string,bank_code:string,bank_office_code:string,byproduct_code:string,cg_siret:string,client_num:string,client_num_key:string,client_type:string,closing_date:string,due_date:string,holder:string,id_of_account:string,identifier:string,legal_entity:string,main_advisor:string,main_third_id:string,mtaupa:string,mtaure:string,mtnovb3:string,mtnovb9:string,opening_date:string,pm_bad_id:string,currency_code:string,product_code:string,reason_close_insurance:string,rib_key:string,signe_amount_of_outstanding:string,signe_mtaupa:string,signe_mtaure:string,signe_mtnovb3:string,signe_mtnovb9:string,source_code:string,status:string,top_sbb:string,top_vp_vd:string,role_tiers_presta:string>>,
    pp array<struct<code_nation:string,code_pays_nais:string,code_pays_rfis:string,code_pays_rmon:string,first_name:string,fonction_interv:string,identifier:string,last_name:string,pourc_detention:string,authority_other:string,authority_account:string,authority_securities:string,ref_procuration:string,sous_type_indiv:string,type_de_lien:string,type_de_lien_economique:string,nature_droits:string,statut_tiers_pp:string,type_tiers_pp:string>>,
    pm array<struct<identifier:string,legal_name_90c:string,type_de_lien:string,type_de_lien_economique:string,nature_droits:string,pourc_detention:string,rct_identifier:string,statut_tiers_pm:string,type_tiers_pm:string>>,
    duplicates_count bigint)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/grc_pp_bv';

DROP TABLE IF EXISTS ${hivevar:cbsBase}.grc_pm_bv;
CREATE EXTERNAL TABLE ${hivevar:cbsBase}.grc_pm_bv(
    headquarters_address string,
    siren_identifier string,
    client_type string,
    relationship_type_status string,
    compliance_3c_top string,
    private_bank_top string,
    incident_nature string,
    ind_risk_lab string,
    psa_code string,
    indiv_subtype string,
    client_incident string,
    headquarters_address_title string,
    headquarters_address_diverse string,
    headquarters_address_diverse_suppl string,
    main_agency string,
    trade_as_name string,
    end_relationship_reason_code string,
    ca_export_year string,
    ca_ht string,
    relationship_end_date string,
    dec_code string,
    dr_code string,
    headquarters_address_country_code string,
    headquarters_address_numero_voie string,
    dec_name string,
    dr_name string,
    employees_total string,
    company_creation_date string,
    identifier string,
    rct_identifier string,
    relationship_start_date string,
    risk_label string,
    sign_ca_export_year string,
    sign_ca_ht string,
    private_bank_top_update_date string,
    city string,
    inactive_client_top string,
    legal_category_code string,
    csm_code string,
    legal_form string,
    naf_code string,
    nationality_country_code string,
    nsm_code string,
    country_tax_code string,
    currency_country_code string,
    pcru_code string,
    ssr_code string,
    risk_cotation_code string,
    bddf_crs_status string,
    monaco_crs_status string,
    ind_risk_lab_update_date string,
    last_update_bddf_crs_status string,
    last_update_crs_status_monaco string,
    risk_loss_relationship_date string,
    tax_id string,
    interdict_bdf_code string,
    assets_country_localisation string,
    headquarters_address_locality string,
    legal_name_90c string,
    risk_loss_relationship string,
    privileged_phone string,
    dom_top string,
    sbb_top string,
    headquarters_address_code_postal string,
    presta array<struct<address_type:string,amount_of_outstanding:string,bank_code:string,bank_office_code:string,byproduct_code:string,cg_siret:string,client_num:string,client_num_key:string,client_type:string,closing_date:string,due_date:string,holder:string,id_of_account:string,identifier:string,legal_entity:string,main_advisor:string,main_third_id:string,mtaupa:string,mtaure:string,mtnovb3:string,mtnovb9:string,opening_date:string,pm_bad_id:string,currency_code:string,product_code:string,reason_close_insurance:string,rib_key:string,signe_amount_of_outstanding:string,signe_mtaupa:string,signe_mtaure:string,signe_mtnovb3:string,signe_mtnovb9:string,source_code:string,status:string,top_sbb:string,top_vp_vd:string,role_tiers_presta:string>>,
    pp array<struct<code_nation:string,code_pays_nais:string,code_pays_rfis:string,code_pays_rmon:string,first_name:string,fonction_interv:string,identifier:string,last_name:string,pourc_detention:string,authority_other:string,authority_account:string,authority_securities:string,ref_procuration:string,sous_type_indiv:string,type_de_lien:string,type_de_lien_economique:string,nature_droits:string,statut_tiers_pp:string,type_tiers_pp:string>>,
    pm array<struct<identifier:string,legal_name_90c:string,type_de_lien:string,type_de_lien_economique:string,nature_droits:string,pourc_detention:string,rct_identifier:string,statut_tiers_pm:string,type_tiers_pm:string>>,
    add_holder array<struct<name:string,percentage:string>>,
    holder array<struct<name:string,percentage:string,type:string>>,
    customer_relationship_manager struct<agency:string,email:string,phone_number:string,last_name:string>,
    chief_executive_officer struct<name:string,second_chief_name:string>,
    duplicates_count bigint)
PARTITIONED BY (yyyy STRING, mm STRING, dd STRING)
STORED AS PARQUET
LOCATION '${hivevar:hdfsDataOutputLocation}/grc_pm_bv';