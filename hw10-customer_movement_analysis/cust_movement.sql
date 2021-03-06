
WITH TEMP_CUST AS (

    SELECT 
        *,
        IF (CUST_TYPE = "CHURN", DATE_ADD(TRAN_DATE, INTERVAL 90 DAY), TRAN_DATE) AS TRAN_DATE_NEW,
        DATE_TRUNC(TRAN_DATE, MONTH) AS YEAR_MONTH,
        DATE_TRUNC(IF (CUST_TYPE = "CHURN", DATE_ADD(TRAN_DATE, INTERVAL 90 DAY), TRAN_DATE) , MONTH) AS YEAR_MONTH_NEW
        
    FROM( 
        SELECT 
            CUST_CODE,
            TRAN_DATE,
            FISRT_TIME,
            LAST_TIME,
            LEAD_TIME,
            MAX_DATE,
            CASE WHEN TRAN_DATE = FISRT_TIME THEN "NEW"
                 WHEN DATE_DIFF( TRAN_DATE, LAST_TIME, DAY) >= 90 THEN "REACTIVATED"
                 -- WHEN DATE_DIFF( LEAD_TIME, TRAN_DATE, DAY) >= 90 AND LEAD_TIME = MAX_DATE THEN "CHURN"
                 WHEN DATE_DIFF( LEAD_TIME, TRAN_DATE, DAY) >= 90 THEN "CHURN"
                 ELSE "REPEAT" END AS CUST_TYPE

        FROM (
            SELECT
                CUST_CODE,
                TRAN_DATE,
                MIN(TRAN_DATE) OVER (PARTITION BY CUST_CODE ORDER BY TRAN_DATE) AS FISRT_TIME,
                LAG(TRAN_DATE) OVER (PARTITION BY CUST_CODE ORDER BY TRAN_DATE) AS LAST_TIME,
                COALESCE( LEAD(TRAN_DATE) OVER (PARTITION BY CUST_CODE ORDER BY TRAN_DATE), MAX(TRAN_DATE) OVER() ) AS LEAD_TIME,
                MAX(TRAN_DATE) OVER() AS MAX_DATE

            FROM( 
                SELECT 
                    PARSE_DATE('%Y%m%d', CAST(SHOP_DATE AS STRING)) AS TRAN_DATE,
                    CUST_CODE
                FROM `crmanalytics-308203.crm_analytics_superstore.superstore`
                WHERE CUST_CODE IS NOT NULL
            )
        )
    )
)
    
SELECT 
    YEAR_MONTH, 
    YEAR_MONTH_NEW,
    CUST_TYPE,
    COUNT(DISTINCT CUST_CODE) AS NO_CUST

FROM TEMP_CUST
GROUP BY YEAR_MONTH, YEAR_MONTH_NEW, CUST_TYPE


