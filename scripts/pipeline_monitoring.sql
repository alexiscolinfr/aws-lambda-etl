WITH logs AS (
    SELECT
        GROUP_CONCAT(DISTINCT SUBSTRING_INDEX(pl.pipe_name, '.', -1)) AS "pipe_name",
        SUM(pl.status_id = 0)-1 AS "retry",
        IF(SUM(pl.manual_trigger) > 0,TRUE,FALSE) AS "manual_trigger",
        MIN(IF(pl.status_id = 0,pl.created_at,NULL)) AS "starting_date",
        MAX(IF(pl.status_id <> 0,pl.created_at,NULL)) AS "ending_date",
        MAX(IF(pl.status_id <> 0,SEC_TO_TIME(pl.duration), NULL)) AS "duration",
         CASE
            WHEN SUM(pl.status_id = 1) > 0 THEN 'Success'
            WHEN SUM(pl.status_id = 2) > 0 THEN 'Error'
            WHEN MAX(pl.status_id) = 0 AND MIN(pl.created_at) < DATE_SUB(NOW(), INTERVAL 45 MINUTE) THEN 'Timed out'
            ELSE 'Running'
        END AS "status",
        MAX(IF(pl.status_id <> 0, pl.extracted_rows, NULL)) AS "extracted_rows",
        MAX(IF(pl.status_id <> 0, pl.loaded_rows, NULL)) AS "loaded_rows",
        MAX(IF(pl.status_id <> 0, pl.dataframes_memory, NULL)) AS "memory_used",
        GROUP_CONCAT(DISTINCT pl.error) AS "error_message"
    FROM 
        pipeline_logs pl
    GROUP BY 
        pl.uuid
)

SELECT
    ROW_NUMBER() OVER(ORDER BY l.starting_date) AS "ID",
    l.pipe_name AS "Pipe name",
    l.retry AS "Retry",
    l.manual_trigger AS "Manual trigger",
    l.starting_date AS "Starting date",
    l.ending_date AS "Ending date",
    l.duration AS "Duration",
    l.status AS "Status",
    l.extracted_rows AS "Extracted rows",
    l.loaded_rows AS "Loaded rows",
    l.memory_used AS "Memory used",
    l.error_message AS "Error message"
FROM
    logs l
WHERE 
    starting_date >= DATE_SUB(CURDATE(), INTERVAL 24 HOUR)
ORDER BY 
    l.starting_date DESC