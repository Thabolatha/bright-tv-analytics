SELECT
  *
FROM
  "TV_SET"."PUBLIC"."SHOWSTOPPER_1"
  
LIMIT
  20;
  ------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM "TV_SET"."PUBLIC"."LUNGA"
LIMIT 20;

DESCRIBE TABLE TV_SET.PUBLIC.LUNGA;

DESCRIBE TABLE TV_SET.PUBLIC.SHOWSTOPPER_1;
----------------------------------------------------------------------------------------------------------------------------

SELECT 
    L.USERID,
    L.CHANNEL2,
    L.RECORDDATE2,
    L.DURATION2,
    U.*
FROM TV_SET.PUBLIC.LUNGA AS L
LEFT JOIN TV_SET.PUBLIC.SHOWSTOPPER_1 AS U
    ON L.USERID = U.USERID
LIMIT 20;

------------------------------------------------------------------------------------------------------------------------------
SELECT
  COUNT(*) AS total_sessions,
  SUM(duration_min)::FLOAT/60.0 AS total_watch_hours,
  COUNT(DISTINCT user_id) AS unique_users,
  AVG(duration_min) AS avg_session_min
FROM ANALYTICS_BRIGHTTV_SESSIONS_SA;
SELECT 'LUNGA' AS tbl, COUNT(*) AS rows FROM TV_SET.PUBLIC.LUNGA
UNION ALL
SELECT 'SHOWSTOPPER_1', COUNT(*) FROM TV_SET.PUBLIC.SHOWSTOPPER_1;
SELECT * 
FROM "TV_SET"."PUBLIC"."LUNGA"
LIMIT 20;

SELECT * 
FROM "TV_SET"."PUBLIC"."SHOWSTOPPER_1"
LIMIT 20;

----------------------------------------------------------------------------------------------------------------------------


WITH clean_usage AS (
    SELECT
        USERID,
        CHANNEL2 AS channel,
        
        -- Convert RECORDDATE2 to timestamp and to SA time
        DATEADD(
            hour, 
            2, 
            TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')
        ) AS sa_time,
        
        -- Convert Duration to seconds
        DATE_PART(hour,   TO_TIME(DURATION2)) * 3600 +
        DATE_PART(minute, TO_TIME(DURATION2)) * 60 +
        DATE_PART(second, TO_TIME(DURATION2)) AS duration_seconds,
        
        -- Derived fields
        TO_CHAR(
            DATEADD(hour, 2, TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')),
            'YYYY-MM-DD'
        ) AS day,
        
        TO_CHAR(
            DATEADD(hour, 2, TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')), 
            'DY'
        ) AS weekday,
        
        TO_CHAR(
            DATEADD(hour, 2, TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')),
            'HH24'
        ) AS hour
    FROM "TV_SET"."PUBLIC"."LUNGA"
)
SELECT * FROM clean_usage ;
-------------------------------------------------------------------------------------------------------------------------------

--gender distribution
SELECT
  gender, 
  COUNT(*) AS users
  FROM "TV_SET"."PUBLIC"."SHOWSTOPPER_1"
  GROUP BY gender;
-----------------------------------------------------------------------------------------------------------------------------
--AGE GROUPS
  SELECT
    CASE
       WHEN age < 18 THEN 'Under 18 (teenage)'
        WHEN age BETWEEN 18 AND 24 THEN '18-24 (youth)'
        WHEN age BETWEEN 25 AND 34 THEN '25-34 (young adults)'
        WHEN age BETWEEN 35 AND 44 THEN '35-44 (adults)'
        ELSE '45+ (elderly)'
    END AS age_group,
    COUNT(*) AS USERS
    FROM"TV_SET"."PUBLIC"."SHOWSTOPPER_1"
    GROUP BY age_group
    ORDER BY users DESC;
---------------------------------------------------------------------------------------------------------------------------------

-- PROVINCE DISTRIBUTION
    SELECT 
      PROVINCE,
      COUNT(*) AS USERS
      FROM"TV_SET"."PUBLIC"."SHOWSTOPPER_1"
      GROUP BY PROVINCE
      ORDER BY USERS DESC;
-----------------------------------------------------------------------------------------------------------------------------


-- VIEWERSHIP BY AGE GROUP
WITH clean_usage AS (
    SELECT
        USERID,
        CHANNEL2,
        --  Convert RECORDDATE2 (string) to timestamp, then from UTC to SA time
        CONVERT_TIMEZONE(
            'UTC',
            'Africa/Johannesburg',
            TO_TIMESTAMP_NTZ(RECORDDATE2, 'YYYY/MM/DD HH24:MI')
        ) AS start_time_sa,

        -- 2) Convert DURATION2 (HH:MI:SS) to seconds
        DATE_PART('hour',   TO_TIME(DURATION2)) * 3600
      + DATE_PART('minute', TO_TIME(DURATION2)) * 60
      + DATE_PART('second', TO_TIME(DURATION2))      AS duration_seconds
    FROM "TV_SET"."PUBLIC"."LUNGA"
),

age_groups AS (
    SELECT
        USERID,
        CASE
            WHEN age < 18 THEN 'Under 18 (teenage)'
        WHEN age BETWEEN 18 AND 24 THEN '18-24 (youth)'
        WHEN age BETWEEN 25 AND 34 THEN '25-34 (young adults)'
        WHEN age BETWEEN 35 AND 44 THEN '35-44 (adults)'
        ELSE '45+ (elderly)'
        END AS age_group
    FROM "TV_SET"."PUBLIC"."SHOWSTOPPER_1"
)

SELECT
    a.age_group,
    SUM(c.duration_seconds) / 3600.0     AS total_hours,
    COUNT(DISTINCT c.USERID)            AS unique_viewers
FROM age_groups a
JOIN clean_usage c
    ON a.USERID = c.USERID
GROUP BY a.age_group
ORDER BY total_hours DESC;


-----------------------------------------------------------------------------------------------------------------------------

--DAILY CONSUMPTION TREND
WITH clean_usage AS (
    SELECT
        USERID,
        DATEDIFF('second', TIME '00:00:00', TO_TIME(DURATION2)) AS duration_seconds,
        
        
        TO_CHAR(
            DATEADD(hour, 2, TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')),
            'DD Mon YYYY'
        ) AS day
    FROM TV_SET.PUBLIC.LUNGA
)

SELECT
    day,
    SUM(duration_seconds) / 3600 AS total_hours,
    COUNT(*) AS sessions,
    COUNT(DISTINCT USERID) AS active_users
FROM clean_usage
GROUP BY day
ORDER BY TO_DATE(day, 'DD Mon YYYY');

-----------------------------------------------------------------------------------------------------------------------------
-- DAY OF THE WEEK usage
WITH clean_usage AS (
    SELECT
        USERID,
        CHANNEL2,
        -- convert string to timestamp 
        DATEADD(
            hour, 
            2,
            TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')
        ) AS start_time_sa,

        -- day of week (Mon, Tue…)
        TO_CHAR(
            DATEADD(hour, 2, TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')),
            'DY'
        ) AS weekday,

        -- convert HH:MM:SS to seconds
        DATEDIFF(
            'second',
            TIME '00:00:00',
            TO_TIME(DURATION2)
        ) AS duration_seconds
    FROM "TV_SET"."PUBLIC"."LUNGA"
)

SELECT
    weekday,
    SUM(duration_seconds) / 3600 AS total_hours
FROM clean_usage
GROUP BY weekday
ORDER BY total_hours DESC;


-----------------------------------------------------------------------------------------------------------------------------
-- HOURLY usage TREND
WITH clean_usage AS (
    SELECT
        USERID,
        CHANNEL2,
        -- convert record date to timestamp and add 2 hours to convert UTC 
        DATEADD(
            hour,
            2,
            TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')
        ) AS start_time_sa,

        -- extract hour of day
        TO_CHAR(
            DATEADD(hour, 2, TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')),
            'HH24'
        ) AS hour,

        -- convert duration text 00:00:00 → seconds
        DATEDIFF(
            'second',
            TIME '00:00:00',
            TO_TIME(DURATION2)
        ) AS duration_seconds
    FROM "TV_SET"."PUBLIC"."LUNGA"
)

SELECT
    hour,
    SUM(duration_seconds) / 3600 AS total_hours
FROM clean_usage
GROUP BY hour
ORDER BY hour;

----------------------------------------------------------------------------------------------------------------------------
-- CHANNEL PERFORMANCE

WITH clean_usage AS (
    SELECT
        USERID,
        CHANNEL2 AS channel,
        -- Convert RECORDDATE2 to SA timestamp
        DATEADD(
            hour,
            2,
            TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')
        ) AS start_time_sa,

        -- Convert duration (HH:MM:SS) to seconds
        DATEDIFF(
            'second',
            TIME '00:00:00',
            TO_TIME(DURATION2)
        ) AS duration_seconds
    FROM "TV_SET"."PUBLIC"."LUNGA"
)

SELECT
    channel,
    SUM(duration_seconds) / 3600 AS hours,
    COUNT(*) AS sessions,
    COUNT(DISTINCT USERID) AS viewers
FROM clean_usage
GROUP BY channel
ORDER BY hours DESC;




-----------------------------------------------------------------------------------------------------------------------------

   
    
