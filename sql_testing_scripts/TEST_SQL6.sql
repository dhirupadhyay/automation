TRUNCATE TABLE
  `{datalake_project_id}.{database_insight}.anlc_viva_engage_graphapi_groups_members`;
INSERT INTO
  `{datalake_project_id}.{database_insight}.anlc_viva_engage_graphapi_groups_members`
WITH
  TEMP AS (
  SELECT
    DISTINCT CAST(ReportDate AS date) AS report_date,
    Community_ID,
    CAST(NULLIF(Community_Feed_ID,' ') AS integer) AS community_feed_id,
    Community_Name,
    Community_Type,
    `{datalake_project_id}.{broadcast_mrdr}.mask_pii`(SPLIT(UserPrincipalName,"@")[
    OFFSET
      (0)]) AS EID,
    Role
  FROM
    `{datalake_project_id}.{viva_raw}.air_103875_futureenterprise_graphapi_groups_members` FG)
SELECT
  DISTINCT TEMP.* EXCEPT (EID),
  CP.peoplekey AS peoplekey
FROM
  TEMP
LEFT JOIN
  `{datalake_project_id}.{database_insight}.cur_peoplehierachyinfor` CP
ON
  TEMP.EID = CP.enterpriseid;