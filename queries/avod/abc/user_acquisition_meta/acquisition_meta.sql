WITH 
    user_base AS (
        SELECT dw_device_id 
        FROM acquisition.masterusertablesince2019 
        GROUP BY 1
    ),

    wv_temp AS (
        SELECT dw_device_id,
            SUM(CASE WHEN wv.category = 'ENT' THEN wv.wt ELSE 0 END) AS ent_wt,
            SUM(CASE WHEN wv.category = 'SPORTS' THEN wv.wt ELSE 0 END) AS sports_wt,
            SUM(CASE WHEN wv.category = 'NEWS' THEN wv.wt ELSE 0 END) AS news_wt
        FROM acquisition.watched_video_temp wv
        GROUP BY dw_device_id
    ), 

    first_content AS (
        SELECT dw_device_id,
               content_id,
               category,
               content_type,
               channel,
               genre,
               language,
               title,
               episode_name,
               platform,
               cd,
               SUM(wt) AS wt,
               ROW_NUMBER() OVER (PARTITION BY dw_device_id ORDER BY SUM(wt) DESC) AS content_rank
        FROM acquisition.watched_video_temp
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ),

    carrier AS (
        SELECT dw_device_id,
            CASE
                WHEN (LOWER(carrier) LIKE '%jio%'
                      OR LOWER(carrier_hs) LIKE '%jio%'
                      OR LOWER(partner_access) LIKE '%jio%') THEN 'jio'
                WHEN LOWER(platform) = 'jio-lyf' THEN 'jio'
                WHEN (LOWER(carrier) LIKE '%airtel%'
                      OR LOWER(carrier_hs) LIKE '%airtel%') THEN 'airtel'
                WHEN (LOWER(carrier) LIKE '%voda%'
                      OR LOWER(carrier_hs) LIKE '%voda%') THEN 'vodafone'
                WHEN (LOWER(carrier) LIKE '%idea%'
                      OR LOWER(carrier_hs) LIKE '%idea%') THEN 'idea'
                WHEN (LOWER(carrier) LIKE '%dea%'
                      OR LOWER(carrier_hs) LIKE '%dea%') THEN 'idea'
                WHEN (LOWER(carrier) LIKE '%bsnl%'
                      OR LOWER(carrier_hs) LIKE '%bsnl%') THEN 'bsnl'
                WHEN (LOWER(carrier) LIKE '%tata%'
                      OR LOWER(carrier_hs) LIKE '%tata%') THEN 'tata'
                WHEN (LENGTH(carrier) = 0
                      AND LENGTH(carrier_hs) = 0) THEN 'null'
                ELSE 'others'
            END AS carrier
        FROM acquisition.watched_video_temp
        GROUP BY 1, 2
    ),

    utm_campaign AS (
        SELECT dw_device_id,
               utm_campaign,
               utm_source,
               cd
        FROM acquisition.watched_video_temp
        GROUP BY 1, 2, 3, 4
    ),

    referrer_tray AS (
        SELECT dw_device_id,
            CASE
                WHEN referrer_tray_name LIKE 'Continue Watching' THEN 'Continue Watching'
                WHEN referrer_tray_name LIKE 'Popular%' THEN 'Popular trays'
                WHEN referrer_tray_name LIKE 'Spotlight' THEN 'Spotlight'
                WHEN referrer_tray_name LIKE 'You May Also Like' THEN 'You May Also Like'
                WHEN referrer_tray_name LIKE 'Top Picks For You' THEN 'Top Picks For You'
                WHEN LOWER(referrer_tray_name) LIKE '%masthead%' THEN 'Masthead'
                ELSE referrer_tray_name
            END AS referrer_tray
        FROM acquisition.watched_video_temp
        GROUP BY 1, 2
    )

INSERT OVERWRITE TABLE acquisition.user_acquisition_meta
PARTITION (category, acq_month, cd) 
SELECT 
    first_content.dw_device_id,
    first_content.platform,
    first_content.content_id,
    first_content.content_type,
    first_content.channel,
    first_content.genre,
    first_content.language,
    first_content.title,
    first_content.episode_name,
    CONCAT_WS(',', COLLECT_SET(car.carrier)) AS carrier_list,
    CONCAT_WS(',', COLLECT_SET(utm.utm_campaign)) AS utm_campaign_list,
    CONCAT_WS(',', COLLECT_SET(utm.utm_source)) AS utm_source_list,
    CONCAT_WS(',', COLLECT_SET(referrer_tray.referrer_tray)) AS referrer_tray_list,
    SUM(first_content.wt) AS content_wt,
    SUM(wv.ent_wt) AS ent_wt,
    SUM(wv.sports_wt) AS sports_wt,
    SUM(wv.news_wt) AS news_wt,
    COALESCE(first_content.category, 'na') AS category,
    TRUNC(first_content.cd, 'MM') AS acq_month,
    first_content.cd AS cd
FROM 
    first_content
INNER JOIN 
    carrier car ON car.dw_device_id = first_content.dw_device_id
INNER JOIN 
    utm_campaign utm ON utm.dw_device_id = first_content.dw_device_id
INNER JOIN 
    referrer_tray ON referrer_tray.dw_device_id = first_content.dw_device_id
INNER JOIN 
    wv_temp wv ON wv.dw_device_id = first_content.dw_device_id 
LEFT JOIN 
    user_base ON user_base.dw_device_id = first_content.dw_device_id
LEFT JOIN 
    acquisition.user_acquisition_meta uam ON uam.dw_device_id = first_content.dw_device_id
WHERE 
    first_content.content_rank = 1
    AND (uam.dw_device_id IS NULL AND user_base.dw_device_id IS NULL)
GROUP BY 
    1, 2, 3, 4, 5, 6, 7, 8, 9, 18, 19, 20;
