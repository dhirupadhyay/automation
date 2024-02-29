CREATE OR REPLACE TABLE
  transient.narhub_usretail_weekly_aggregates_sls_dim_period AS
SELECT
  *
FROM (
  WITH
    all_time_monthly_time_periods AS (
    SELECT
      fiscal_month_abbreviation_desc,
      fiscal_year_nbr,
      fiscal_year_month_nbr,
      fiscal_year_week_nbr,
      fiscal_quarter_nbr,
      fiscal_quarter_in_year_nbr,
      fiscal_month_in_year_nbr,
      fiscal_week_in_year_nbr,
      fiscal_week_in_month_nbr,
      CONCAT("WEEK ENDING"," ",FORMAT_DATE('%x',DATE_SUB(fiscal_week_end_dt, INTERVAL 1 DAY))) AS nielsen_gmi_fiscal_week_tag_us_desc,
      '1 w/e '|| SUBSTR(CAST(DATE_SUB(CAST(fiscal_week_end_dt AS date), INTERVAL 1 DAY) AS string),6,2)||'/' ||SUBSTR(CAST(DATE_SUB(CAST(fiscal_week_end_dt AS date), INTERVAL 1 DAY) AS string),9,2)||'/' ||SUBSTR(CAST(DATE_SUB(CAST(fiscal_week_end_dt AS date), INTERVAL 1 DAY) AS string),3,2) AS week_desc
    FROM
      `edw-prd-e567f9.enterprise.dim_date`
    WHERE
      language_cd = 'EN'
      AND fiscal_year_variant_cd = '07'
      AND fiscal_horiz_first_day_of_week_flg='Y'
      AND fiscal_year_nbr>2019),
    fiscal_flags AS (
    SELECT
      MAX(fiscal_year_nbr) AS max_fiscal_year_nbr,
      MAX(fiscal_year_month_nbr) AS max_fiscal_year_month_nbr,
      MAX(fiscal_year_week_nbr) AS max_fiscal_year_week_nbr,
      MIN(CASE
          WHEN t1.fiscal_week_end_dt=t2.thirteen_weeks_ago THEN fiscal_year_week_nbr
      END
        ) AS min_fiscal_year_week_nbr,
      MIN(CASE
          WHEN t1.fiscal_week_end_dt=t2.one_year_ago THEN fiscal_year_week_nbr
      END
        ) AS min52wk_fiscal_year_week_nbr,
      MAX(fiscal_quarter_nbr) AS max_fiscal_quarter_nbr,
      MAX(fiscal_week_end_dt) AS max_fiscal_week_end_dt
    FROM
      `edw-prd-e567f9.enterprise.dim_date` t1
    INNER JOIN (
      SELECT
        DATE_SUB(CAST(fiscal_week_end_dt AS date), INTERVAL 2 WEEK) AS two_weeks_ago,
        DATE_SUB(CAST(fiscal_week_end_dt AS date), INTERVAL 14 WEEK) AS thirteen_weeks_ago,
        DATE_SUB(CAST(fiscal_week_end_dt AS date), INTERVAL 53 WEEK) AS one_year_ago,
      FROM
        `edw-prd-e567f9.enterprise.dim_date`
      WHERE
        language_cd = 'EN'
        AND fiscal_year_variant_cd = '07'
        AND fiscal_horiz_first_day_of_week_flg='Y'
        AND fiscal_week_current_flg = 'Y' ) t2
    ON
      (t1.fiscal_week_end_dt = t2.two_weeks_ago
        OR t1.fiscal_week_end_dt = t2.thirteen_weeks_ago
        OR t1.fiscal_week_end_dt=t2.one_year_ago) --Deals WITH delay IN nielsen DATA
      AND t1.language_cd = 'EN'
      AND t1.fiscal_year_variant_cd = '07'
      AND t1.fiscal_horiz_first_day_of_week_flg='Y'),
    fiscal_flags_with_formatting AS (
    SELECT
      fiscal_month_abbreviation_desc,
      max_fiscal_year_nbr,
      max_fiscal_year_month_nbr,
      max_fiscal_year_week_nbr,
      max_fiscal_quarter_nbr,
      min52wk_fiscal_year_week_nbr,
      FORMAT_DATE('%x',DATE_SUB(max_fiscal_week_end_dt, INTERVAL 1 DAY)) AS nielsen_gmi_fiscal_week_end_dt,
      nielsen_gmi_fiscal_week_tag_us_desc,
      fiscal_week_in_month_nbr
    FROM
      all_time_monthly_time_periods t1
    INNER JOIN
      fiscal_flags t2
    ON
      t1.fiscal_year_week_nbr = t2.max_fiscal_year_week_nbr),
    WEEKLY_CURRENT_WEEK AS (
    SELECT
      'WEEK' AS period_type,
      REGEXP_REPLACE(t2.nielsen_gmi_fiscal_week_tag_us_desc, 'WEEK', 'PERIOD')||" ("||UPPER(t2.fiscal_month_abbreviation_desc)||" WK "||CAST(t2.fiscal_week_in_month_nbr AS string)||")" AS period_ending,
      UPPER(t2.fiscal_month_abbreviation_desc)||" WK "||CAST(t2.fiscal_week_in_month_nbr AS string) AS period_ending_short_desc,
      week_desc,
      UPPER(t2.fiscal_month_abbreviation_desc) AS fiscal_month_abbreviation_desc,
      fiscal_year_nbr,
      fiscal_quarter_in_year_nbr,
      fiscal_month_in_year_nbr,
      fiscal_week_in_year_nbr,
      t2.fiscal_week_in_month_nbr
    FROM
      all_time_monthly_time_periods t1
    INNER JOIN
      fiscal_flags_with_formatting t2
    ON
      t1.fiscal_year_week_nbr = t2.max_fiscal_year_week_nbr),
    WEEKLY_MTD AS (
    SELECT
      'MTD' AS period_type,
      t2.nielsen_gmi_fiscal_week_end_dt||" ("||UPPER(t2.fiscal_month_abbreviation_desc)||" WK "||CAST(t2.fiscal_week_in_month_nbr AS string)||")" AS period_ending,
      'MTD' AS period_ending_short_desc,
      week_desc,
      UPPER(t2.fiscal_month_abbreviation_desc) AS fiscal_month_abbreviation_desc,
      fiscal_year_nbr,
      fiscal_quarter_in_year_nbr,
      fiscal_month_in_year_nbr,
      NULL AS fiscal_week_in_year_nbr,
      NULL AS fiscal_week_in_month_nbr
    FROM
      all_time_monthly_time_periods t1
    INNER JOIN
      fiscal_flags_with_formatting t2
    ON
      t1.fiscal_year_week_nbr <= t2.max_fiscal_year_week_nbr
      AND t1.fiscal_year_month_nbr = t2.max_fiscal_year_month_nbr),
    WEEKLY_QTD AS (
    SELECT
      'QTD' AS period_type,
      t2.nielsen_gmi_fiscal_week_end_dt||" ("||UPPER(t2.fiscal_month_abbreviation_desc)||" WK "||CAST(t2.fiscal_week_in_month_nbr AS string)||")" AS period_ending,
      'QTD' AS period_ending_short_desc,
      week_desc,
      UPPER(t2.fiscal_month_abbreviation_desc) AS fiscal_month_abbreviation_desc,
      fiscal_year_nbr,
      fiscal_quarter_in_year_nbr,
      NULL AS fiscal_month_in_year_nbr,
      NULL AS fiscal_week_in_year_nbr,
      NULL AS fiscal_week_in_month_nbr
    FROM
      all_time_monthly_time_periods t1
    INNER JOIN
      fiscal_flags_with_formatting t2
    ON
      t1.fiscal_quarter_nbr = t2.max_fiscal_quarter_nbr
      AND t1.fiscal_year_month_nbr <= t2.max_fiscal_year_month_nbr
      AND t1.fiscal_year_week_nbr <= t2.max_fiscal_year_week_nbr),
    WEEKLY_YTD AS (
    SELECT
      'YTD' AS period_type,
      t2.nielsen_gmi_fiscal_week_end_dt||" ("||UPPER(t2.fiscal_month_abbreviation_desc)||" WK "||CAST(t2.fiscal_week_in_month_nbr AS string)||")" AS period_ending,
      'YTD' AS period_ending_short_desc,
      week_desc,
      UPPER(t2.fiscal_month_abbreviation_desc) AS fiscal_month_abbreviation_desc,
      fiscal_year_nbr,
      NULL AS fiscal_quarter_in_year_nbr,
      NULL AS fiscal_month_in_year_nbr,
      NULL AS fiscal_week_in_year_nbr,
      NULL AS fiscal_week_in_month_nbr
    FROM
      all_time_monthly_time_periods t1
    INNER JOIN
      fiscal_flags_with_formatting t2
    ON
      t1.fiscal_year_nbr = t2.max_fiscal_year_nbr
      AND t1.fiscal_year_month_nbr <= t2.max_fiscal_year_month_nbr
      AND t1.fiscal_year_week_nbr <= t2.max_fiscal_year_week_nbr),
    WEEKLY_TRENDING AS (
    SELECT
      'WEEKLY_TRENDING' AS period_type,
      REGEXP_REPLACE(t1.nielsen_gmi_fiscal_week_tag_us_desc, 'WEEK', 'PERIOD')||" ("||UPPER(fiscal_month_abbreviation_desc)||" WK "||CAST(fiscal_week_in_month_nbr AS string)||")" AS period_ending,
      UPPER(fiscal_month_abbreviation_desc)||" WK "||CAST(fiscal_week_in_month_nbr AS string) AS period_ending_short_desc,
      week_desc,
      UPPER(fiscal_month_abbreviation_desc) AS fiscal_month_abbreviation_desc,
      fiscal_year_nbr,
      fiscal_quarter_in_year_nbr,
      fiscal_month_in_year_nbr,
      fiscal_week_in_year_nbr,
      fiscal_week_in_month_nbr
    FROM
      all_time_monthly_time_periods t1
    INNER JOIN
      fiscal_flags t2
    ON
      t1.fiscal_year_week_nbr BETWEEN t2.min_fiscal_year_week_nbr
      AND t2.max_fiscal_year_week_nbr),
    ROLLING_52WEEKS AS (
    SELECT
      '52WK_ROLLING' AS period_type,
      '52 WEEK ENDING ' || UPPER(t2.fiscal_month_abbreviation_desc)||" WK "||CAST(t2.fiscal_week_in_month_nbr AS string) || " " || CAST(t2.max_fiscal_year_nbr AS string) AS period_ending,
      UPPER(t2.fiscal_month_abbreviation_desc)||" WK "||CAST(t2.fiscal_week_in_month_nbr AS string) AS period_ending_short_desc,
      week_desc,
      UPPER(t2.fiscal_month_abbreviation_desc) AS fiscal_month_abbreviation_desc,
      t2.max_fiscal_year_nbr AS fiscal_year_nbr,
      NULL AS fiscal_quarter_in_year_nbr,
      NULL AS fiscal_month_in_year_nbr,
      NULL AS fiscal_week_in_year_nbr,
      NULL AS fiscal_week_in_month_nbr from 
      all_time_monthly_time_periods t1
    INNER JOIN
      fiscal_flags_with_formatting t2
    ON
      t1.fiscal_year_week_nbr BETWEEN t2.min52wk_fiscal_year_week_nbr
      AND t2.max_fiscal_year_week_nbr)
  SELECT
    *
  FROM
    WEEKLY_CURRENT_WEEK union all select *
  FROM
    WEEKLY_MTD union all select *
  FROM
    WEEKLY_QTD union all select *
  FROM
    WEEKLY_YTD union all select *
  FROM
    WEEKLY_TRENDING union all select *
  FROM
    ROLLING_52WEEKS)