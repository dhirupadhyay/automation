CREATE OR REPLACE VIEW
  `{project_id}.{dataset_id}.svoc_vw_mms_master` AS
WITH
  MMS AS (
  SELECT
    campaign_member_sk,
    campaign_sk,
    contact_sk,
    COALESCE(A.campaign_id,B.campaign_id) AS campaign_id_mms,
    COALESCE(C.client_contact_id,B.contact_id) AS client_contact_id_mms,
    CASE
      WHEN LOWER(TRIM(c.mailing_country)) IN (LOWER('Australia'), LOWER('Christmas Island'), LOWER('New Zealand'), LOWER('Cook Islands'), LOWER('Cocos (keeling) Islands')) THEN 'ANZ'
      WHEN LOWER(TRIM(c.mailing_country)) IN (LOWER('United States'),
      LOWER('Puerto Rico'),
      LOWER('Canada'),
      LOWER('US Virgin Islands')) THEN 'NA'
      WHEN LOWER(TRIM(c.mailing_country)) IN (LOWER('Israel'), LOWER('Japan')) THEN UPPER(TRIM(C.mailing_country))
    ELSE
    'OTHERS'
  END
    AS legal_market_mms_co,
    CASE
      WHEN LOWER(TRIM(C.marketing_opt_reason)) IN (LOWER('Direct mail preference set to opted out in earlier database'), LOWER('Email preference set to opted out in earlier database'), LOWER('On Hold for Opt In (GDPR)'), LOWER('Unsubscribed from Cvent'), LOWER('Requested Removal from all Marketing databases (GDPR)'), LOWER('Pending GDPR Notification Email'), LOWER('Unsubscribed to all Accenture communications')) THEN 'Exclude'
    ELSE
    'Include'
  END
    AS mkt_opt_reason_flag_mms_co,
    TRIM(C.mailing_country) AS mailing_country_mms_co,
    B.campaign_mem_id AS campaign_mem_id_mms_cm,
    B.campaign_name AS campaign_name_mms_cm,
    B.contact_id AS contact_id_mms_cm,
    B.company_or_account AS company_or_account_mms_cm,
    B.country AS country_mms_cm,
    B.created_date AS created_date_mms_cm,
    B.cvent_event_ind AS cvent_event_ind_mms_cm,
    B.do_not_call AS do_not_call_mms_cm,
    B.do_not_contact AS do_not_contact_mms_cm,
    B.has_opted_out_of_email AS has_opted_out_of_email_mms_cm,
    B.first_responded_date AS first_responded_date_mms_cm,
    B.lead_id AS lead_id_mms_cm,
    B.lead_or_contact AS lead_or_contact_mms_cm,
    B.lead_and_campaign_id AS lead_and_campaign_id_mms_cm,
    B.member_status_text AS member_status_text_mms_cm,
    B.numberof_total_clicks AS numberof_total_clicks_mms_cm,
    B.numberof_unique_clicks AS numberof_unique_clicks_mms_cm,
    B.numberof_unique_opens AS numberof_unique_opens_mms_cm,
    B.has_responded AS has_responded_mms_cm,
    B.is_active AS is_active_mms_cm,
    B.account_industry AS account_industry_mms_cm,
    B.ingestion_ts AS ingestion_ts_mms_cm,
    A.brand_maker_id AS brand_maker_id_mms_ca,
    A.ability_to_influence AS ability_to_influence_mms_ca,
    A.accenture_wrk_event AS accenture_wrk_event_mms_ca,
    A.account AS account_mms_ca,
    A.accounts_client_classification AS accounts_client_classification_mms_ca,
    A.account_industry AS account_industry_mms_ca,
    A.account_ou AS account_ou_mms_ca,
    A.activation_cost AS activation_cost_mms_ca,
    A.actual_contract_sign_date AS actual_contract_sign_date_mms_ca,
    A.actual_cost AS actual_cost_mms_ca,
    A.agencies AS agencies_mms_ca,
    A.agency_pointof_contact AS agency_pointof_contact_mms_ca,
    A.budgeted_cost AS budgeted_cost_mms_ca,
    A.buyer_function_targeted AS buyer_function_targeted_mms_ca,
    A.currency_iso_code AS currency_iso_code_mms_ca,
    A.campaign_idlower_case AS campaign_idlower_case_mms_ca,
    A.name AS name_mms_ca,
    A.campaign_type AS campaign_type_mms_ca,
    A.ccm AS ccm_mms_ca,
    A.channel AS channel_mms_ca,
    A.number_of_contacts AS number_of_contacts_mms_ca,
    A.contract_end_date AS contract_end_date_mms_ca,
    A.contract_start_date AS contract_start_date_mms_ca,
    A.number_of_converted_leads AS number_of_converted_leads_mms_ca,
    A.craccount_cgnm AS craccount_cgnm_mms_ca,
    A.deal_stage AS deal_stage_mms_ca,
    A.description AS description_mms_ca,
    A.event_description1 AS event_description1_mms_ca,
    A.end_date AS end_date_mms_ca,
    A.event_country AS event_country_mms_ca,
    A.event_format AS event_format_mms_ca,
    A.events_operating_unit AS events_operating_unit_mms_ca,
    A.event_nm AS event_nm_mms_ca,
    A.event_owner AS event_owner_mms_ca,
    A.event_platform AS event_platform_mms_ca,
    A.event_tier AS event_tier_mms_ca,
    A.expected_contract_signing_dt AS expected_contract_signing_dt_mms_ca,
    A.expected_response AS expected_response_mms_ca,
    A.expected_revenue AS expected_revenue_mms_ca,
    A.hierarchy_expected_revenue AS hierarchy_expected_revenue_mms_ca,
    A.financial_cust AS financial_cust_mms_ca,
    A.financial_cust_country AS financial_cust_country_mms_ca,
    A.financial_cust_country_name AS financial_cust_country_name_mms_ca,
    A.financial_cust_industry AS financial_cust_industry_mms_ca,
    A.financial_cust_industry_subsegment AS financial_cust_industry_subsegment_mms_ca,
    A.financial_cust_operating_unit AS financial_cust_operating_unit_mms_ca,
    A.gdpremail_type AS gdpremail_type_mms_ca,
    A.geo_marketing_lead AS geo_marketing_lead_mms_ca,
    A.email_sent_ind AS email_sent_ind_mms_ca,
    A.is_cvent_event AS is_cvent_event_mms_ca,
    A.number_of_leads AS number_of_leads_mms_ca,
    A.level_category AS level_category_mms_ca,
    A.operating_grp AS operating_grp_mms_ca,
    A.marketing_tactics AS marketing_tactics_mms_ca,
    A.marketing_team AS marketing_team_mms_ca,
    A.target_cg AS target_cg_mms_ca,
    A.target_og AS target_og_mms_ca,
    A.target_ou AS target_ou_mms_ca,
    A.mega_deal AS mega_deal_mms_ca,
    A.mmscampaign_id AS mmscampaign_id_mms_ca,
    A.nggmoperating_group_nm AS nggmoperating_group_nm_mms_ca,
    A.nggmtarget_ognm AS nggmtarget_ognm_mms_ca,
    A.ocmlevel AS ocmlevel_mms_ca,
    A.number_of_opportunities AS number_of_opportunities_mms_ca,
    A.opportunity_og AS opportunity_og_mms_ca,
    A.opportunity_ou AS opportunity_ou_mms_ca,
    A.opportunity_usdamount AS opportunity_usdamount_mms_ca,
    A.percentageof_responses AS percentageof_responses_mms_ca,
    A.primary_accenture_growth_priority AS primary_accenture_growth_priority_mms_ca,
    A.primary_agency AS primary_agency_mms_ca,
    A.primary_country AS primary_country_mms_ca,
    A.primary_industry AS primary_industry_mms_ca,
    A.opportunity_amount AS opportunity_amount_mms_ca,
    A.primary_opportunity_amount AS primary_opportunity_amount_mms_ca,
    A.opportunity_id AS opportunity_id_mms_ca,
    A.opportunity_idname AS opportunity_idname_mms_ca,
    A.record_type_nm AS record_type_nm_mms_ca,
    A.start_date AS start_date_mms_ca,
    A.status AS status_mms_ca,
    A.accenture_business AS accenture_business_mms_ca,
    A.total_no_of_contacts AS total_no_of_contacts_mms_ca,
    A.total_no_of_leads AS total_no_of_leads_mms_ca,
    A.total_no_responded AS total_no_responded_mms_ca,
    A.type AS type_mms_ca,
    A.amount_all_opportunities AS amount_all_opportunities_mms_ca,
    A.is_active AS is_active_mms_ca,
    A.ingestion_ts AS ingestion_ts_mms_ca,
    C.accenture_alumni AS accenture_alumni_mms_co,
    C.accenture_com_subscriber_country AS accenture_com_subscriber_country_mms_co,
    C.accenture_relationship_strength AS accenture_relationship_strength_mms_co,
    C.account_name AS account_name_mms_co,
    C.account_client_classification AS account_client_classification_mms_co,
    C.account_client_group AS account_client_group_mms_co,
    C.account_id AS account_id_mms_co,
    C.account_og AS account_og_mms_co,
    C.account_primary_industry AS account_primary_industry_mms_co,
    C.acnrelationship_strength_number AS acnrelationship_strength_number_mms_co,
    C.active_contact AS active_contact_mms_co,
    C.client_satisfaction_classification_desc AS client_satisfaction_classification_desc_mms_co,
    C.client_satisfaction_desc AS client_satisfaction_desc_mms_co,
    C.client_satisfaction_last_response_dt AS client_satisfaction_last_response_dt_mms_co,
    C.client_satisfaction_score_nbr AS client_satisfaction_score_nbr_mms_co,
    C.contact_data_quality_score AS contact_data_quality_score_mms_co,
    C.contact_data_quality_score_nbr AS contact_data_quality_score_nbr_mms_co,
    C.contact_marketing_id AS contact_marketing_id_mms_co,
    C.conversion_existing_account AS conversion_existing_account_mms_co,
    C.converted_lead AS converted_lead_mms_co,
    C.converted_lead_id AS converted_lead_id_mms_co,
    C.created_date AS created_date_mms_co,
    C.date_joined AS date_joined_mms_co,
    C.dateof_exit AS dateof_exit_mms_co,
    C.department AS department_mms_co,
    C.do_not_contact_ind AS do_not_contact_ind_mms_co,
    C.eloqua_score AS eloqua_score_mms_co,
    C.etinterests AS etinterests_mms_co,
    C.etsubscriptions AS etsubscriptions_mms_co,
    C.financial_customer_name AS financial_customer_name_mms_co,
    C.has_opted_out_of_email AS has_opted_out_of_email_mms_co,
    C.iv_inside_view_data_integrity_status AS iv_inside_view_data_integrity_status_mms_co,
    C.iv_inside_view_match_status AS iv_inside_view_match_status_mms_co,
    C.ivmatch_score AS ivmatch_score_mms_co,
    C.key_decision_maker AS key_decision_maker_mms_co,
    C.last_campaign_end_dt AS last_campaign_end_dt_mms_co,
    C.lead_origination AS lead_origination_mms_co,
    C.lead_source AS lead_source_mms_co,
    C.market AS market_mms_co,
    C.marketing_opt_contact AS marketing_opt_contact_mms_co,
    C.marketing_opt_date AS marketing_opt_date_mms_co,
    C.marketing_opt_reason AS marketing_opt_reason_mms_co,
    C.market_unit AS market_unit_mms_co,
    C.preferred_language AS preferred_language_mms_co,
    C.priority_contact AS priority_contact_mms_co,
    C.sales_do_not_contact AS sales_do_not_contact_mms_co,
    C.viewin_contact360desc AS viewin_contact360desc_mms_co,
    C.is_active AS is_active_mms_co,
    C.ingestion_ts AS ingestion_ts_mms_co,
    C.email_flag AS email_flag_mms_co,
    C.preference_center_token_or_purId AS preference_center_token_or_purid_mms_co,
    C.ot_universal_id AS ot_universal_id_mms_co,
    C.data_enrichment AS data_enrichment_mms_co,
    C.reporting_consent AS reporting_consent_mms_co,
    C.topics AS topics_mms_co,
    C.channel_statement AS channel_statement_mms_co,
    C.current_email_subscriptions AS current_email_subscriptions_mms_co,
    C.industries AS industries_mms_co,
    C.informational_statement AS informational_statement_mms_co,
    C.gdpr_client_categorization AS gdpr_client_categorization_mms_co,
    STRING(NULL) AS contact_level_mms_co,
    STRING(NULL) AS interests_mms_co,
    NULL AS can_include_in_marketing_communications_mms_co,
    C.glr_compliance_mms_co,
    C.gdpr_compliance_mms_co,
    CURRENT_DATETIME('UTC') AS processed_dttm
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *,
        CASE
          WHEN mailing_country NOT IN ("Austria", "Germany", "Switzerland") THEN TRUE
        ELSE
        FALSE
      END
        AS glr_compliance_mms_co,
        CASE
          WHEN (received_gdpr_email IS TRUE AND marketing_opt_contact != "Opted OUT" AND email_flag = TRUE) THEN TRUE
        ELSE
        FALSE
      END
        AS gdpr_compliance_mms_co
      FROM
        `prd-65343-datalake-bd-88394358.mms_20158_in.tbl_contact`
      WHERE
        client_contact_id NOT IN (
        SELECT
          DISTINCT id
        FROM
          `prd-65343-datalake-bd-88394358.mms_20158_in.tbl_deletedcontacts`) )
    WHERE
      (glr_compliance_mms_co = TRUE
        AND gdpr_compliance_mms_co = TRUE
        AND UPPER(reporting_consent) NOT IN ('NO'))
      OR (glr_compliance_mms_co = TRUE
        AND gdpr_compliance_mms_co = TRUE
        AND reporting_consent IS NULL) )C
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, contact_id ORDER BY created_date) AS dup_row_no
      FROM
        `prd-65343-datalake-bd-88394358.mms_20158_in.tbl_campaignmember`
      WHERE
        campaign_id IN (
        SELECT
          DISTINCT campaign_id
        FROM
          `prd-65343-datalake-bd-88394358.mms_20158_in.vw_campaign`
        WHERE
          Start_Date >= '2021-09-01'
          AND LOWER(Campaign_Type) = LOWER('Marketing')) )
    WHERE
      dup_row_no = 1 ) B
  ON
    UPPER(C.Client_Contact_ID) = UPPER(B.Contact_Id)
  LEFT JOIN (
    SELECT
      *
    FROM
      `prd-65343-datalake-bd-88394358.mms_20158_in.vw_campaign`
    WHERE
      Start_Date >= '2021-09-01'
      AND LOWER(Campaign_Type) = LOWER('Marketing') ) A
  ON
    UPPER(B.Campaign_Id) = UPPER(A.Campaign_ID)
  LEFT JOIN
    `prd-65343-datalake-bd-88394358.mcdatalakesvoc_245974_in.svoc_tbl_campaignmember_lookup` D
  ON
    UPPER(B.Campaign_ID) = UPPER(D.campaign_id_mms)
    AND UPPER(B.Contact_Id) = UPPER(D.client_contact_id_mms)
  WHERE
    (UPPER(C.Account_Name) NOT LIKE '%ACCENT%'
      AND UPPER(C.Account_Name) NOT LIKE '%INTERNAL%') ),
  IPC AS (
  SELECT
    DISTINCT campaign_sk,
    ipcid_IPC,
    campaign_unique_identifier_IPC,
    primary_campaign_theme_ipc,
    secondary_campaign_theme_ipc,
    theme_ipc
  FROM
    `prd-65343-datalake-bd-88394358.mcdatalakesvoc_245974_in.svoc_vw_ipc_master` ),
  AI_DUMP AS (
  SELECT
    DISTINCT id,
    ipcid,
    campaign_type AS campaign_type_ai,
    title AS title_ai,
    campaign_description AS campaign_description_ai,
    campaign_status AS campaign_status_ai,
    campaign_unique_identifier AS campaign_unique_identifier_ai,
    launch_date AS launch_date_ai,
    end_date AS end_date_ai,
    created AS created_ai,
    modified AS modified_ai,
    ingestion_ts AS ingestion_ts_ai,
    comment_text AS comment_text_ai,
    result_labels AS result_labels_ai,
    Use_digital_technology_to_improve_health_and_care_delivery__Digital_Health_ AS use_digital_technology_to_improve_health_and_care_delivery_digital_health_ai,
    Modernizing_tech_infrastructure_to_improve_the_digital_core AS modernizing_tech_infrastructure_to_improve_the_digital_core_ai,
    Creating_purpose_driven_cultures AS creating_purpose_driven_cultures_ai,
    Reimagine_products_via_digital_engineering_and_manufacturing AS reimagine_products_via_digital_engineering_and_manufacturing_ai,
    Quantum_computing AS quantum_computing_ai,
    Drive_growth_and_maintain_profitability AS drive_growth_and_maintain_profitability_ai,
    Accelerate_sustainable_mobility AS accelerate_sustainable_mobility_ai,
    Improved_insurance_claim_experiences_and_operations_using_data_and_AI AS improved_insurance_claim_experiences_and_operations_using_data_and_ai_ai,
    Access__create__and_unlock_talent AS access_create_and_unlock_talent_ai,
    Use_technology_to_enhance_operational_performance AS use_technology_to_enhance_operational_performance_ai,
    Shifting_to_the_new_retail_experience_and_consumer_behavior AS shifting_to_the_new_retail_experience_and_consumer_behavior_ai,
    Improving_national_security_around_the_world AS improving_national_security_around_the_world_ai,
    Grow_through_M_A_and_restructuring AS grow_through_m_a_and_restructuring_ai,
    Technology_applied_to_science_innovation AS technology_applied_to_science_innovation_ai,
    Improved_digital_payment_experiences_and_operations AS improved_digital_payment_experiences_and_operations_ai,
    Maximizing_value_in_the_shift_to_5G AS maximizing_value_in_the_shift_to_5g_ai,
    Space_based_technologies AS space_based_technologies_ai,
    Maximize_growth_across_sales_and_commerce_channels AS maximize_growth_across_sales_and_commerce_channels_ai,
    Grow_by_shifting_to_new_business_models AS grow_by_shifting_to_new_business_models_ai,
    Driving_business_transformation_with_data_and_AI AS driving_business_transformation_with_data_and_ai_ai,
    Grow_by_becoming_a_customer_centric_organization_across_channels AS grow_by_becoming_a_customer_centric_organization_across_channels_ai,
    New_revenue_streams_in_the_metaverse AS new_revenue_streams_in_the_metaverse_ai,
    Advances_in_digital_core_technologies AS advances_in_digital_core_technologies_ai,
    Sustainable_raw_materials_for_product_innovation AS sustainable_raw_materials_for_product_innovation_ai,
    Designing_organizations_for_their_reinvention AS designing_organizations_for_their_reinvention_ai,
    Delivering_on_the_energy_transition AS delivering_on_the_energy_transition_ai,
    Expanding_Cloud_capabilities_to_unlock_more_value AS expanding_cloud_capabilities_to_unlock_more_value_ai,
    Reducing_risk_and_building_a_safer_organization_through_security AS reducing_risk_and_building_a_safer_organization_through_security_ai,
    Sustainability_as_core_strategy AS sustainability_as_core_strategy_ai,
    Create_resilient__relevant__and_sustainable_supply_chains AS create_resilient_relevant_and_sustainable_supply_chains_ai,
    residual_topics AS residual_topics_ai
  FROM
    `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.tbl_campaign_tagged`)
SELECT
  MMS.*,
  IPC.ipcid_IPC,
  IPC.campaign_unique_identifier_IPC,
  IPC.primary_campaign_theme_ipc,
  IPC.secondary_campaign_theme_ipc,
  IPC.theme_ipc,
  AI_DUMP.result_labels_ai,
  AI_DUMP.use_digital_technology_to_improve_health_and_care_delivery_digital_health_ai,
  AI_DUMP.modernizing_tech_infrastructure_to_improve_the_digital_core_ai,
  AI_DUMP.creating_purpose_driven_cultures_ai,
  AI_DUMP.reimagine_products_via_digital_engineering_and_manufacturing_ai,
  AI_DUMP.quantum_computing_ai,
  AI_DUMP.drive_growth_and_maintain_profitability_ai,
  AI_DUMP.accelerate_sustainable_mobility_ai,
  AI_DUMP.improved_insurance_claim_experiences_and_operations_using_data_and_ai_ai,
  AI_DUMP.access_create_and_unlock_talent_ai,
  AI_DUMP.use_technology_to_enhance_operational_performance_ai,
  AI_DUMP.shifting_to_the_new_retail_experience_and_consumer_behavior_ai,
  AI_DUMP.improving_national_security_around_the_world_ai,
  AI_DUMP.grow_through_m_a_and_restructuring_ai,
  AI_DUMP.technology_applied_to_science_innovation_ai,
  AI_DUMP.improved_digital_payment_experiences_and_operations_ai,
  AI_DUMP.maximizing_value_in_the_shift_to_5g_ai,
  AI_DUMP.space_based_technologies_ai,
  AI_DUMP.maximize_growth_across_sales_and_commerce_channels_ai,
  AI_DUMP.grow_by_shifting_to_new_business_models_ai,
  AI_DUMP.driving_business_transformation_with_data_and_ai_ai,
  AI_DUMP.grow_by_becoming_a_customer_centric_organization_across_channels_ai,
  AI_DUMP.new_revenue_streams_in_the_metaverse_ai,
  AI_DUMP.advances_in_digital_core_technologies_ai,
  AI_DUMP.sustainable_raw_materials_for_product_innovation_ai,
  AI_DUMP.designing_organizations_for_their_reinvention_ai,
  AI_DUMP.delivering_on_the_energy_transition_ai,
  AI_DUMP.expanding_cloud_capabilities_to_unlock_more_value_ai,
  AI_DUMP.reducing_risk_and_building_a_safer_organization_through_security_ai,
  AI_DUMP.sustainability_as_core_strategy_ai,
  AI_DUMP.create_resilient_relevant_and_sustainable_supply_chains_ai,
  AI_DUMP.residual_topics_ai
FROM
  MMS
LEFT JOIN
  IPC
ON
  MMS.campaign_sk = IPC.campaign_sk
LEFT JOIN
  AI_DUMP
ON
  IPC.ipcid_IPC = AI_DUMP.ipcid