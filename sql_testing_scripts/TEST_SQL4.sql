WITH
  cte1 AS (
  SELECT
    DISTINCT lastactivity_date,
    report_date,
    name,
    community_feed_id,
    community_type,
    member_count,
    admin_count,
    graph_posted_count,
    graph_read_count,
    graph_liked_count,
    discussion_count,
    question_count,
    praise_count,
    poll_count,
    announcement_count,
    comment_count
  FROM (
    SELECT
      *
    FROM
      `{datalake_project_id}.{database_insight}.stg_viva_engage_groups_with_activity`
    WHERE
      -- created_datetime >= ( --SELECT
      -- DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 5 QUARTER),month)) --(Commenting OUT temporarily TO include ALL the DATA AND TO avoid missing 
      -- OUT ON communities IN analytical layer) 
    UPPER(name) NOT LIKE '%TEST%'
    AND member_count >= 30 )),
  cte2 AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY community_feed_id, name, report_date ORDER BY lastactivity_date DESC) AS row_num,
    report_date,
    name,
    community_feed_id,
    community_type,
    member_count,
    admin_count,
    graph_posted_count,
    graph_read_count,
    graph_liked_count,
    discussion_count,
    question_count,
    praise_count,
    poll_count,
    announcement_count,
    comment_count
  FROM
    cte1 ),
  cte3 AS (
  SELECT
    report_date,
    name,
    community_feed_id,
    community_type,
    member_count,
    admin_count,
    graph_posted_count,
    graph_read_count,
    graph_liked_count,
    discussion_count,
    question_count,
    praise_count,
    poll_count,
    announcement_count,
    comment_count
  FROM
    cte2
  WHERE
    row_num = 1),
  cte4 AS (
  SELECT
    DISTINCT community_feed_id,
    name,
    ROW_NUMBER() OVER (PARTITION BY community_feed_id ORDER BY report_date DESC) AS row_num
  FROM
    `{datalake_project_id}.{database_insight}.stg_viva_engage_groups_with_activity`),
  cte5 AS (
  SELECT
    DISTINCT community_feed_id,
    name
  FROM
    cte4
  WHERE
    row_num = 1)
SELECT
  cte3.report_date,
  cte5.name,
  cte3.community_feed_id,
  cte3.community_type,
  cte3.member_count,
  cte3.admin_count,
  cte3.graph_posted_count,
  cte3.graph_read_count,
  cte3.graph_liked_count,
  cte3.discussion_count,
  cte3.question_count,
  cte3.praise_count,
  cte3.poll_count,
  cte3.announcement_count,
  cte3.comment_count
FROM
  cte3
LEFT JOIN
  cte5
ON
  cte3.community_feed_id = cte5.community_feed_id
LEFT JOIN cte4
ON cte4.community_feed_id = cte5.community_feed_id
LEFT JOIN cte3
ON cte5.community_feed_id = cte3.community_feed_id;