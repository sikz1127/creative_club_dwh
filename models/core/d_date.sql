{{
  config(
    materialized = "table",
    alias        = "D_DATE",
    pre_hook = [
      "ALTER SESSION SET WEEK_START = 1",
      "ALTER SESSION SET WEEK_OF_YEAR_POLICY = 1"
    ]
  )
}}

-- ---------------------------------------------------
-- 2) Populate via MERGE + Snowflake GENERATOR
-- ---------------------------------------------------

{% set start_date = var('start_date', '1990-01-01') %}
{% set days       = var('number_days', 70 * 365) %}



SELECT TO_NUMBER(TO_CHAR(MYDATE, 'yyyymmdd'))               AS DWH_DATE_ID,
                        CAST(NULL AS NUMBER)                                    JULIAN_DAY,
                        MYDATE                                               AS DATE_TIME_START,
                        MYDATE + INTERVAL '23 hours, 59 minutes, 59 seconds' AS DATE_TIME_END,
                        TO_CHAR(MYDATE, 'dd-MON-yyyy')                       AS DATE_VALUE,
                        CAST(NULL AS NUMBER)                                    DAY_TIME_SPAN,
                        DATEADD(DAY, -1, MYDATE)                             AS PREVIOUSDAY,
                        DATEADD(DAY, +1, MYDATE)                             AS NEXTDAY,
                        DAYOFWEEK(MYDATE)                                    AS DAY_OF_WEEK_NUMBER,
                        DECODE(extract('dayofweek_iso', MYDATE),
                               1, 'Monday',
                               2, 'Tuesday',
                               3, 'Wednesday',
                               4, 'Thursday',
                               5, 'Friday',
                               6, 'Saturday',
                               7,
                               'Sunday')                                     AS DAY_OF_WEEK_DESC,
                        DAYNAME(MYDATE)                                      AS DAY_OF_WEEK_SDESC,
                        CAST(NULL AS NUMBER)                                    DAYS_IN_WEEK,
                        CASE
                            WHEN DAYOFWEEK(MYDATE) IN
                                 (6, 7)
                                THEN
                                1
                            ELSE
                                0
                            END
                                                                             AS WEEKEND_FLAG,
                        DATE_PART(WEEK, MYDATE) - DATE_PART(WEEK, DATE_TRUNC('MONTH', MYDATE)) +
                        1                                                    AS WEEK_IN_MONTH_NUMBER,
                        DATE_PART(WEEK, MYDATE) - DATE_PART(WEEK, DATE_TRUNC('YEAR', MYDATE)) +
                        1                                                    AS WEEK_IN_YEAR_NUMBER,

                        CAST(NULL AS NUMBER)                                    YEAR_WEEK_COL,
                        DATE_TRUNC('week', MYDATE)                           AS WEEK_START_DATE,
                        DATEADD(DAY, 6, DATE_TRUNC('week', MYDATE)) +
                        INTERVAL '23 hours, 59 minutes, 59 seconds'          AS WEEK_END_DATE,
                        WEEKISO(MYDATE)                                      AS ISO_WEEK_NUMBER,
                        CAST(NULL AS NUMBER)                                 AS ISO_WEEK_START_DATE,
                        CAST(NULL AS NUMBER)                                 AS ISO_WEEK_END_DATE,
                        DAYOFMONTH(MYDATE)                                   AS DAY_OF_MONTH_NUMBER,
                        TO_CHAR(MYDATE, 'MM')                                AS MONTH_VALUE,
                        TO_CHAR(MYDATE, 'yyyy') || TO_CHAR(MYDATE, 'MM')
                                                                                YYYYMM_MONTH_VALUE,
                        CAST(NULL AS NUMBER)                                    YEAR_MONTH_COL,
                        TO_CHAR(MYDATE, 'MMMM')                              AS MONTH_DESC,
                        TO_CHAR(MYDATE, 'MON')                               AS MONTH_SDESC,
                        TRUNC(MYDATE, 'mm')                                  AS MONTH_START_DATE,
                        LAST_DAY(TRUNC(MYDATE, 'mm'))                        AS MONTH_LAST_DATE,
                        DATEDIFF('day', TRUNC(MYDATE, 'QUARTER'), MYDATE) + 1
                                                                             AS DAY_OF_QUARTER_NUMBER,
                        QUARTER(MYDATE)                                      AS QUARTER_VALUE,

                        TO_NUMBER(TO_CHAR(MYDATE, 'yyyy') || TO_CHAR(QUARTER(MYDATE)))
                                                                             AS YEAR_QUARTER_COL,
                        DECODE(extract('QUARTER', MYDATE),
                               1, 'Q1',
                               2, 'Q2',
                               3, 'Q3',
                               4,
                               'Q4')                                         as QUARTER,
                        TRUNC(MYDATE, 'Q')                                   AS QUARTER_START_DATE,
                        DATEADD(DAY, 89, TRUNC(MYDATE, 'QUARTER')) +
                        INTERVAL '23 hours, 59 minutes, 59 seconds'          AS QUARTER_END_DATE,
                        DATEDIFF('day', TRUNC(MYDATE, 'QUARTER'), ADD_MONTHS(TRUNC(MYDATE, 'QUARTER'), 3))
                                                                             AS DAYS_IN_QUARTER,
                        CAST(NULL AS NUMBER)                                    QUARTER_TIME_SPAN,
                        DAYOFYEAR(MYDATE)                                    AS DAY_OF_YEAR_NUMBER,
                        TO_CHAR(MYDATE, 'yyyy')                              AS YEAR_VALUE,
                        'YR' || TO_CHAR(MYDATE, 'yyyy')                      AS YEAR_DESC,
                        'YR' || TO_CHAR(MYDATE, 'yy')                        AS YEAR_SDESC,
                        TRUNC(MYDATE, 'Y')                                   AS YEAR_START_DATE,
                        DATEADD(DAY, 364, TRUNC(MYDATE, 'Y')) + INTERVAL '23 hours, 59 minutes, 59 seconds'
                                                                             AS YEAR_END_DATE,
                        DATEDIFF('day', TRUNC(MYDATE, 'Y'), ADD_MONTHS(TRUNC(MYDATE, 'Y'), 12))
                                                                             AS DAYS_IN_YEAR,
                        CASE
                            WHEN MYDATE = LAST_DAY(TRUNC(MYDATE, 'mm'))
                                THEN
                                1
                            ELSE
                                0
                            END
                                                                             AS LAST_DAY_OF_MONTH_FLAG,
                        CAST(NULL AS NUMBER)                                    WEEKDAYS_TO_GO_IN_MONTH,

                        CAST(NULL AS VARCHAR2 (255 CHAR))
                                                                                YEAR_WEEK_COL_DESC,
                        CAST(NULL AS VARCHAR2 (255 CHAR))
                                                                                YEAR_MONATH_COL_DESC,
                        CAST(NULL AS VARCHAR2 (255 CHAR))
                                                                                YEAR_QUARTER_COL_DESC,
                        CURRENT_TIMESTAMP as DWH_CREATION_TS         ,
                        CURRENT_TIMESTAMP as DWH_MODIFICATION_TS ,
                        CURRENT_DATE as DWH_START_DATE          ,
                        CURRENT_DATE as DWH_END_DATE            ,
                        true as DWH_CURRENT_FLAG        ,
                        CURRENT_USER() as DWH_CREATION_USER       ,
                        CURRENT_USER() as DWH_MODIFICATION_USER   ,
                        CURRENT_ROLE() as DWH_CREATION_ROLE       ,
                        CURRENT_ROLE() as DWH_MODIFICATION_ROLE   ,
                        TO_NUMBER(TO_CHAR(CONVERT_TIMEZONE('Europe/Berlin', current_timestamp), 'YYYYMMDDHH24MISS')) as DWH_TRACE_RUN_ID
                 FROM (SELECT DATEADD(DAY, (row_number() over (order by seq4()) - 1), TO_DATE('{{ start_date }}')) AS MYDATE
                       FROM TABLE (generator(rowcount => {{ days }})))
