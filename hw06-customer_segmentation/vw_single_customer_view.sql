SELECT
    CUST_CODE,
    COUNT(DISTINCT bas_morning) AS bas_morning,
    COUNT(DISTINCT bas_afternoon) AS bas_afternoon,
    COUNT(DISTINCT bas_night) AS bas_night,
    
    SUM(bas_spend_morning) AS bas_spend_morning,
    SUM(bas_spend_afternoon) AS bas_spend_afternoon,
    SUM(bas_spend_night) AS bas_spend_night,
    SUM(SPEND) AS ttl_spend,
    SUM(QUANTITY) AS ttl_qualtity,
    COUNT(DISTINCT year) AS active_year,
    COUNT(DISTINCT year_month) AS active_month,
    COUNT(DISTINCT SHOP_DATE ) AS ttl_visit,
    COUNT(DISTINCT BASKET_ID) AS ttl_basket,

    COUNT(DISTINCT weekend) AS weekend,
    COUNT(DISTINCT weekday) AS weekday,

    SUM(weekend_spend) AS weekend_spend,
    SUM(weekday_spend) AS weekday_spend,
    COUNT(DISTINCT BASKET_S) AS BASKET_S,
    COUNT(DISTINCT BASKET_M) AS BASKET_M,
    COUNT(DISTINCT BASKET_L) AS BASKET_L,

    COUNT(DISTINCT bas_lessaff) AS bas_lessaff,
    COUNT(DISTINCT bas_midmarket) AS bas_midmarket,
    COUNT(DISTINCT bas_upmarket) AS bas_upmarket,
    COUNT(DISTINCT bas_unclass) AS bas_unclass,

    COUNT(DISTINCT cus_lessaff) AS cus_lessaff,
    COUNT(DISTINCT cus_midmarket) AS cus_midmarket,
    COUNT(DISTINCT cus_upmarket) AS cus_upmarket,
    COUNT(DISTINCT cus_unclass) AS cus_unclass,

    COUNT(DISTINCT bas_type_small) AS bas_type_small,
    COUNT(DISTINCT bas_type_topup) AS bas_type_topup,
    COUNT(DISTINCT bas_type_full) AS bas_type_full,
    COUNT(DISTINCT bas_type_unclass) AS bas_type_unclass,

    COUNT(DISTINCT bas_dom_fresh) AS bas_dom_fresh,
    COUNT(DISTINCT bas_dom_grocery) AS bas_dom_grocery,
    COUNT(DISTINCT bas_dom_mix) AS bas_dom_mix,
    COUNT(DISTINCT bas_dom_nonfood) AS bas_dom_nonfood,
    COUNT(DISTINCT bas_dom_unclass) AS bas_dom_unclass,

    SUM(bas_dom_spend_fresh) AS bas_dom_spend_fresh,
    SUM(bas_dom_spend_grocery) AS bas_dom_spend_grocery,
    SUM(bas_dom_spend_mix) AS bas_dom_spend_mix,
    SUM(bas_dom_spend_nonfood) AS bas_dom_spend_nonfood,
    SUM(bas_dom_spend_unclass) AS bas_dom_spend_unclass,


FROM (
    SELECT 
    CUST_CODE,
    SHOP_DATE,
    SHOP_HOUR,
    IF(SHOP_HOUR < 12, BASKET_ID, NULL ) AS bas_morning,
    IF(SHOP_HOUR < 18, BASKET_ID, NULL ) AS bas_afternoon,
    IF(SHOP_HOUR >= 18, BASKET_ID, NULL ) AS bas_night,

    IF(SHOP_HOUR < 12, SPEND, NULL ) AS bas_spend_morning,
    IF(SHOP_HOUR < 18, SPEND, NULL ) AS bas_spend_afternoon,
    IF(SHOP_HOUR >= 18, SPEND, NULL ) AS bas_spend_night,
    QUANTITY,
    SPEND,
    LEFT(CAST(SHOP_DATE AS STRING),4) AS year,
    SUBSTR(CAST(SHOP_DATE AS STRING), 5,2) AS month,
    SUBSTR(CAST(SHOP_DATE AS STRING), 7,2) AS day,
    LEFT(CAST(SHOP_DATE AS STRING),6) AS year_month,
    BASKET_ID,
    IF(SHOP_WEEKDAY IN (1,7), BASKET_ID, NULL ) AS weekend,
    IF(SHOP_WEEKDAY NOT IN (1,7), BASKET_ID, NULL ) AS weekday,

    IF(SHOP_WEEKDAY IN (1,7), SPEND, NULL ) AS weekend_spend,
    IF(SHOP_WEEKDAY NOT IN (1,7), SPEND, NULL ) AS weekday_spend,

    BASKET_SIZE,
    IF(BASKET_SIZE = 'S', BASKET_ID, NULL) AS BASKET_S,
    IF(BASKET_SIZE = 'M', BASKET_ID, NULL) AS BASKET_M,
    IF(BASKET_SIZE = 'L', BASKET_ID, NULL) AS BASKET_L,
    BASKET_PRICE_SENSITIVITY,
    IF(BASKET_PRICE_SENSITIVITY = 'LA', BASKET_ID, NULL) AS bas_lessaff,
    IF(BASKET_PRICE_SENSITIVITY = 'MM', BASKET_ID, NULL) AS bas_midmarket,
    IF(BASKET_PRICE_SENSITIVITY = 'UM', BASKET_ID, NULL) AS bas_upmarket,
    IF(BASKET_PRICE_SENSITIVITY = 'XX', BASKET_ID, NULL) AS bas_unclass,
    CUST_PRICE_SENSITIVITY,
    IF(CUST_PRICE_SENSITIVITY = 'LA', BASKET_ID, NULL) AS cus_lessaff,
    IF(CUST_PRICE_SENSITIVITY = 'MM', BASKET_ID, NULL) AS cus_midmarket,
    IF(CUST_PRICE_SENSITIVITY = 'UM', BASKET_ID, NULL) AS cus_upmarket,
    IF(CUST_PRICE_SENSITIVITY = 'XX', BASKET_ID, NULL) AS cus_unclass,
    BASKET_TYPE,
    IF(BASKET_TYPE = 'Small Shop', BASKET_ID, NULL) AS bas_type_small,
    IF(BASKET_TYPE = 'Top Up', BASKET_ID, NULL) AS bas_type_topup,
    IF(BASKET_TYPE = 'Full Shop', BASKET_ID, NULL) AS bas_type_full,
    IF(BASKET_TYPE = 'XX', BASKET_ID, NULL) AS bas_type_unclass,
    BASKET_DOMINANT_MISSION,
    IF(BASKET_DOMINANT_MISSION = 'Fresh',BASKET_ID, NULL ) AS bas_dom_fresh,
    IF(BASKET_DOMINANT_MISSION = 'Grocery',BASKET_ID, NULL ) AS bas_dom_grocery,
    IF(BASKET_DOMINANT_MISSION = 'Mixed',BASKET_ID, NULL ) AS bas_dom_mix,
    IF(BASKET_DOMINANT_MISSION = 'Non Food',BASKET_ID, NULL ) AS bas_dom_nonfood,
    IF(BASKET_DOMINANT_MISSION = 'XX',BASKET_ID, NULL ) AS bas_dom_unclass,
    BASKET_DOMINANT_MISSION,
    IF(BASKET_DOMINANT_MISSION = 'Fresh',SPEND, NULL ) AS bas_dom_spend_fresh,
    IF(BASKET_DOMINANT_MISSION = 'Grocery',SPEND, NULL ) AS bas_dom_spend_grocery,
    IF(BASKET_DOMINANT_MISSION = 'Mixed',SPEND, NULL ) AS bas_dom_spend_mix,
    IF(BASKET_DOMINANT_MISSION = 'Non Food',SPEND, NULL ) AS bas_dom_spend_nonfood,
    IF(BASKET_DOMINANT_MISSION = 'XX',SPEND, NULL ) AS bas_dom_spend_unclass,

    -- 20071102 AS SHOP_DATE
FROM `crmanalytics-308203.crm_analytics_superstore.superstore` 
WHERE CUST_CODE IS NOT NULL
)
GROUP BY CUST_CODE
LIMIT 1000