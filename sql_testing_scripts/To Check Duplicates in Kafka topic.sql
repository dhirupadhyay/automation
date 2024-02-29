--To check Duplicates
---------------------------------------------------------------------------------------------------------------------------------------------
--air_103875_futureenterprise_graphapi_groups_members
WITH deduplicated_data AS (
  SELECT
    ReportDate,
    Community_ID,
    Community_Feed_ID,
    Community_Name,
    Community_Type,
    UserPrincipalName,
    Role,
    Country,
    MarketUnit,
    CareerLevel,
    RawingestionDttm,
    ROW_NUMBER() OVER(PARTITION BY 
      ReportDate, Community_ID, Community_Feed_ID, Community_Name,
      Community_Type, UserPrincipalName, Role, Country,
      MarketUnit, CareerLevel, RawingestionDttm
    ) AS CNT
  FROM
    `prd-65343-datalake-bd-88394358.viva_engage_195476_103875_raw.air_103875_futureenterprise_graphapi_groups_members`
)

SELECT
  ReportDate,
  Community_ID,
  Community_Feed_ID,
  Community_Name,
  Community_Type,
  UserPrincipalName,
  Role,
  Country,
  MarketUnit,
  CareerLevel,
  RawingestionDttm
FROM
  deduplicated_data
WHERE
  CNT > 1;
  
 --======================================================================================================================================================================
 --air_103875_futureenterprise_groups_with_activity
 WITH
  Dedup AS (
  SELECT
    ReportDate,
    Name,
    Community_ID,
    Community_Feed_ID,
    Community_Type,
    LastActivityDate,
    CreatedDateTime,
    MemberCount,
    AdminCount,
    Graph_PostedCount,
    Graph_ReadCount,
    Graph_LikedCount,
    DiscussionCount,
    QuestionCount,
    PraiseCount,
    PollCount,
    AnnouncementCount,
    CommentCount,
    RawingestionDttm,
    ROW_NUMBER() OVER (PARTITION BY Name, Community_ID, Community_Feed_ID, Community_Type, LastActivityDate, CreatedDateTime, MemberCount, AdminCount, Graph_PostedCount, Graph_ReadCount, Graph_LikedCount, DiscussionCount, QuestionCount, PraiseCount, PollCount, AnnouncementCount, CommentCount
 ORDER BY RawingestionDttm DESC) AS RNK
  FROM
    `prd-65343-datalake-bd-88394358.viva_engage_195476_103875_raw.air_103875_futureenterprise_groups_with_activity` )
SELECT
  Name,
  Community_ID,
  Community_Feed_ID,
  Community_Type,
  LastActivityDate,
  CreatedDateTime,
  MemberCount,
  AdminCount,
  Graph_PostedCount,
  Graph_ReadCount,
  Graph_LikedCount,
  DiscussionCount,
  QuestionCount,
  PraiseCount,
  PollCount,
  AnnouncementCount,
  CommentCount,
  RawingestionDttm,
  RNK
FROM
  Dedup
WHERE
  RNK>1
  ORDER BY RawingestionDttm DESC
 --======================================================================================================================================================================
 --air_103875_futureenterprise_posts
 WITH
  Dedup AS (
  SELECT
    Post_ID,
    Community_Feed_ID,
    Community_Name,
    Sender_UserPrincipalName,
    Message_Location,
    Message_Type,
    CreatedDate,
    ModifiedDate,
    DeletedDate,
    HasAttachment,
    MainComment_Count,
    SubComment_Count,
    AllComment_Count,
    First_MainComment_CreatedDate,
    Latest_MainComment_CreatedDate,
    First_SubComment_CreatedDate,
    Latest_SubComment_CreatedDate,
    Latest_Comment_CreatedDate,
    RawingestionDttm,
    ROW_NUMBER() OVER (PARTITION BY Post_ID, Community_Feed_ID, Community_Name, Sender_UserPrincipalName, Message_Location, Message_Type, CreatedDate, ModifiedDate, DeletedDate, HasAttachment, MainComment_Count, SubComment_Count, AllComment_Count, First_MainComment_CreatedDate, Latest_MainComment_CreatedDate, First_SubComment_CreatedDate, Latest_SubComment_CreatedDate, Latest_Comment_CreatedDate ORDER BY RawingestionDttm DESC) AS RNK
  FROM
    `prd-65343-datalake-bd-88394358.viva_engage_195476_103875_raw.air_103875_futureenterprise_posts` )
SELECT
  Post_ID,
  Community_Feed_ID,
  Community_Name,
  Sender_UserPrincipalName,
  Message_Location,
  Message_Type,
  CreatedDate,
  ModifiedDate,
  DeletedDate,
  HasAttachment,
  MainComment_Count,
  SubComment_Count,
  AllComment_Count,
  First_MainComment_CreatedDate,
  Latest_MainComment_CreatedDate,
  First_SubComment_CreatedDate,
  Latest_SubComment_CreatedDate,
  Latest_Comment_CreatedDate,
  RawingestionDttm,
  RNK
FROM
  Dedup
WHERE
  RNK>1
ORDER BY
  RNK DESC, RawingestionDttm DESC
 --======================================================================================================================================================================
 --air_103875_futureenterprise_posts_withreply
 WITH
  Dedup AS (
  SELECT
    Post_ID,
    Community_Feed_ID,
    Community_Name,
    Sender_UserPrincipalName,
    Message_Location,
    Message_Type,
    CreatedDate,
    ModifiedDate,
    DeletedDate,
    HasAttachment,
    Comment_id,
    IsSubComment,
    Comment_Sender_UserPrincipalName,
    Comment_CreatedDate,
    Comment_ModifiedDate,
    Comment_DeletedDate,
    RawingestionDttm,
    ROW_NUMBER() OVER (PARTITION BY Post_ID, Community_Feed_ID, Community_Name, Sender_UserPrincipalName, Message_Location, Message_Type, CreatedDate, ModifiedDate, DeletedDate, HasAttachment, Comment_id, IsSubComment, Comment_Sender_UserPrincipalName, Comment_CreatedDate, Comment_ModifiedDate, Comment_DeletedDate ORDER BY RawingestionDttm ) AS RNK
  FROM
    `prd-65343-datalake-bd-88394358.viva_engage_195476_103875_raw.air_103875_futureenterprise_posts_withreply` )
SELECT
  Post_ID,
  Community_Feed_ID,
  Community_Name,
  Sender_UserPrincipalName,
  Message_Location,
  Message_Type,
  CreatedDate,
  ModifiedDate,
  DeletedDate,
  HasAttachment,
  Comment_id,
  IsSubComment,
  Comment_Sender_UserPrincipalName,
  Comment_CreatedDate,
  Comment_ModifiedDate,
  Comment_DeletedDate,
  RawingestionDttm,
  RNK
FROM
  Dedup
WHERE
  RNK>1
ORDER BY
  RNK DESC,
  RawingestionDttm DESC;
 --======================================================================================================================================================================
 --air_103875_futureenterprise_users_activity
 WITH
  dedup AS (
  SELECT
    ReportDate,
    UserPrincipalName,
    Country,
    MarketUnit,
    CareerLevel,
    Graph_PostedCount,
    Graph_ReadCount,
    Graph_LikeCount,
    StoriesCount,
    Community_DiscussionCount,
    Community_QuestionCount,
    Community_PraiseCount,
    Community_PollCount,
    Community_AnnouncementCount,
    Community_CommentCount,
    Storyline_DiscussionCount,
    Storyline_QuestionCount,
    Storyline_PraiseCount,
    Storyline_PollCount,
    Storyline_CommentCount,
    PrivateMessageCount,
    RawingestionDttm,
    ROW_NUMBER() OVER (PARTITION BY ReportDate, UserPrincipalName, Country, MarketUnit, CareerLevel, Graph_PostedCount, Graph_ReadCount, Graph_LikeCount, StoriesCount, Community_DiscussionCount, Community_QuestionCount, Community_PraiseCount, Community_PollCount, Community_AnnouncementCount, Community_CommentCount, Storyline_DiscussionCount, Storyline_QuestionCount, Storyline_PraiseCount, Storyline_PollCount, Storyline_CommentCount, PrivateMessageCount ORDER BY RawingestionDttm) AS RNK
  FROM
    `prd-65343-datalake-bd-88394358.viva_engage_195476_103875_raw.air_103875_futureenterprise_users_activity` )
SELECT
  ReportDate,
  UserPrincipalName,
  Country,
  MarketUnit,
  CareerLevel,
  Graph_PostedCount,
  Graph_ReadCount,
  Graph_LikeCount,
  StoriesCount,
  Community_DiscussionCount,
  Community_QuestionCount,
  Community_PraiseCount,
  Community_PollCount,
  Community_AnnouncementCount,
  Community_CommentCount,
  Storyline_DiscussionCount,
  Storyline_QuestionCount,
  Storyline_PraiseCount,
  Storyline_PollCount,
  Storyline_CommentCount,
  PrivateMessageCount,
  RawingestionDttm,
  RNK
FROM
  dedup
WHERE
  RNK>1
ORDER BY
  RNK DESC,
  RawingestionDttm DESC;
 --======================================================================================================================================================================
 --To find duplicates of 2 tables after UNION, this is for prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Post_Comments_2023-10-28_temp and prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Post_Comments_2023-10-29_temp

 WITH CombinedData AS (
    -- Query for the first table
    SELECT
        Post_ID,
        Community_Feed_ID,
        Community_Name,
        Sender_UserPrincipalName,
        Message_Location,
        Message_Type,
        CreatedDate,
        ModifiedDate,
        DeletedDate,
        HasAttachment,
        Comment_id,
        IsSubComment,
        Comment_Sender_UserPrincipalName,
        Comment_CreatedDate,
        Comment_ModifiedDate,
        Comment_DeletedDate,
        
    FROM
        `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Post_Comments_2023-10-28_temp`

    UNION ALL

    -- Query for the second table
    SELECT
        Post_ID,
        Community_Feed_ID,
        Community_Name,
        Sender_UserPrincipalName,
        Message_Location,
        Message_Type,
        CreatedDate,
        ModifiedDate,
        DeletedDate,
        HasAttachment,
        Comment_id,
        IsSubComment,
        Comment_Sender_UserPrincipalName,
        Comment_CreatedDate,
        Comment_ModifiedDate,
        Comment_DeletedDate,
       
    FROM
        `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Post_Comments_2023-10-29_temp`
)

-- Find duplicate rows based on selected columns
SELECT
    Post_ID,
    Community_Feed_ID,
    Community_Name,
    Sender_UserPrincipalName,
    Message_Location,
    Message_Type,
    CreatedDate,
    ModifiedDate,
    DeletedDate,
    HasAttachment,
    Comment_id,
    IsSubComment,
    Comment_Sender_UserPrincipalName,
    Comment_CreatedDate,
    Comment_ModifiedDate,
    Comment_DeletedDate,
    COUNT(*) AS DuplicateCount
FROM
    CombinedData
GROUP BY
    Post_ID,
    Community_Feed_ID,
    Community_Name,
    Sender_UserPrincipalName,
    Message_Location,
    Message_Type,
    CreatedDate,
    ModifiedDate,
    DeletedDate,
    HasAttachment,
    Comment_id,
    IsSubComment,
    Comment_Sender_UserPrincipalName,
    Comment_CreatedDate,
    Comment_ModifiedDate,
    Comment_DeletedDate
HAVING
    COUNT(*) > 1;
--======================================================================================================================================================================
--To find duplicates from 2 combined tables prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Topics_with_Communities_2023-10-27_temp and prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Topics_with_Communities_2023-10-26_temp
WITH CombinedData AS (
    -- Query for the first table
    SELECT
        Topic_ID,
        Topic_Name,
        Topic_Description,
        Community_Feed_ID,
        Community_Name,
        Post_CreatedDate,
        Post_Count
    FROM
        `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Topics_with_Communities_2023-10-26_temp`

    UNION ALL

    -- Query for the second table
    SELECT
        Topic_ID,
        Topic_Name,
        Topic_Description,
        Community_Feed_ID,
        Community_Name,
        Post_CreatedDate,
        Post_Count
    FROM
        `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Topics_with_Communities_2023-10-27_temp` -- replace with the actual name of your second table
)

-- Find duplicate rows and count
SELECT
    Topic_ID,
    Topic_Name,
    Topic_Description,
    Community_Feed_ID,
    Community_Name,
    Post_CreatedDate,
    Post_Count,
    COUNT(*) AS DuplicateCount
FROM
    CombinedData
GROUP BY
    Topic_ID,
    Topic_Name,
    Topic_Description,
    Community_Feed_ID,
    Community_Name,
    Post_CreatedDate,
    Post_Count
HAVING
    COUNT(*) > 1
    ORDER BY DuplicateCount DESC;

 --======================================================================================================================================================================
 --To find duplicates from one table prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Topics_with_Communities_2023-10-26_temp
With Dups AS 
(Select Topic_ID
,Topic_Name
,Topic_Description
,Community_Feed_ID
,Community_Name
,Post_CreatedDate
,Post_Count
,ROW_NUMBER() OVER (PARTITION BY Topic_ID
,Topic_Name
,Topic_Description
,Community_Feed_ID
,Community_Name
,Post_CreatedDate
,Post_Count) AS RNK FROM `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Topics_with_Communities_2023-10-26_temp`)
SELECT 
Topic_ID
,Topic_Name
,Topic_Description
,Community_Feed_ID
,Community_Name
,Post_CreatedDate
,Post_Count
,RNK FROM Dups WHERE RNK>1
ORDER BY RNK DESC
 ======================================================================================================================================================================
 --To find duplicates from 2 combined tables prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Posts_2023-12-04_temp and prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Posts_2023-12-05_temp
 
 WITH CombinedData AS (
    -- Query for the first table
    SELECT
        Post_ID,
        Community_Feed_ID,
        Community_Name,
        Sender_UserPrincipalName,
        Message_Location,
        Message_Type,
        CreatedDate,
        ModifiedDate,
        DeletedDate,
        HasAttachment,
        MainComment_Count,
        SubComment_Count,
        AllComment_Count,
        First_MainComment_CreatedDate,
        Latest_MainComment_CreatedDate,
        First_SubComment_CreatedDate,
        Latest_SubComment_CreatedDate,
        Latest_Comment_CreatedDate
    FROM
        `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Posts_2023-12-04_temp`

    UNION ALL

    -- Query for the second table (replace with the actual name of your second table)
    SELECT
        Post_ID,
        Community_Feed_ID,
        Community_Name,
        Sender_UserPrincipalName,
        Message_Location,
        Message_Type,
        CreatedDate,
        ModifiedDate,
        DeletedDate,
        HasAttachment,
        MainComment_Count,
        SubComment_Count,
        AllComment_Count,
        First_MainComment_CreatedDate,
        Latest_MainComment_CreatedDate,
        First_SubComment_CreatedDate,
        Latest_SubComment_CreatedDate,
        Latest_Comment_CreatedDate
    FROM
        `prd-65343-datalake-bd-88394358.mktngdiscov_20158_ds.VivaEngage_Posts_2023-12-05_temp`
)

-- Find duplicate rows and count
SELECT
    Post_ID,
    Community_Feed_ID,
    Community_Name,
    Sender_UserPrincipalName,
    Message_Location,
    Message_Type,
    CreatedDate,
    ModifiedDate,
    DeletedDate,
    HasAttachment,
    MainComment_Count,
    SubComment_Count,
    AllComment_Count,
    First_MainComment_CreatedDate,
    Latest_MainComment_CreatedDate,
    First_SubComment_CreatedDate,
    Latest_SubComment_CreatedDate,
    Latest_Comment_CreatedDate,
    COUNT(*) AS DuplicateCount
FROM
    CombinedData
GROUP BY
    Post_ID,
    Community_Feed_ID,
    Community_Name,
    Sender_UserPrincipalName,
    Message_Location,
    Message_Type,
    CreatedDate,
    ModifiedDate,
    DeletedDate,
    HasAttachment,
    MainComment_Count,
    SubComment_Count,
    AllComment_Count,
    First_MainComment_CreatedDate,
    Latest_MainComment_CreatedDate,
    First_SubComment_CreatedDate,
    Latest_SubComment_CreatedDate,
    Latest_Comment_CreatedDate
HAVING
    COUNT(*) > 1
    ORDER BY DuplicateCount DESC;
 --======================================================================================================================================================================