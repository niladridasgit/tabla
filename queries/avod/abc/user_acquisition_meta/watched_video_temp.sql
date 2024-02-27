DROP TABLE acquisition.watched_video_temp;

CREATE TABLE acquisition.watched_video_temp
STORED AS ORC
AS
SELECT 
    dw_device_id,
    a.content_id,
    platform,
    cd,
    carrier,
    carrier_hs,
    partner_access,
    utm_campaign,
    utm_source,
    referrer_tray_name,
    category_def.category,
    category_def.content_type,
    category_def.channel,
    category_def.genre,
    category_def.language,
    category_def.title,
    category_def.episode_name,
    SUM(watch_time) AS wt
FROM 
    data_warehouse.watched_video_daily_aggregates_ist a
LEFT JOIN 
    retention.category_def ON category_def.content_id = a.content_id
WHERE 
    cd = '${hivevar:run_date}'
    AND (play_type = 'watch page' OR play_type IS NULL OR play_type = 'detail page autoplay' OR (play_type = 'autoplay' AND LOWER(referrer_page_title) != 'home'))
    AND (platform IS NOT NULL AND LOWER(platform) != 'null' AND platform != '') AND LOWER(platform) != 'and' AND LOWER(platform) != 'm-web'
    AND LOWER(dw_device_id) != 'null' AND dw_device_id IS NOT NULL
    AND LOWER(dw_device_id) != 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    AND LOWER(dw_device_id) != 'fcdb4b423f4e5283afa249d762ef6aef150e91fccd810d43e5e719d14512dec7'
    AND watch_time > 0 AND watch_time < 1666
GROUP BY 
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;
