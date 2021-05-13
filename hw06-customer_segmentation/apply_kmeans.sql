SELECT 
    * EXCEPT(NEAREST_CENTROIDS_DISTANCE)
FROM ML.PREDICT( 
    MODEL `crmanalytics-308203.crm_analytics_superstore.keams_model`,
    (
        SELECT *
        FROM `crmanalytics-308203.crm_analytics_superstore.vw_customer_singleview`)
    
)