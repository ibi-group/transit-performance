--Script Version: Master - 1.0.0.0

--Check daily schedule adherence for previous service date
--Returns number of routes that are below schedule adherence threshold
--Includes a parameter for the schedule adherence threshold, and only returns an error if route schedule adherence is below [a configurable] percentage

DECLARE @schedule_adherence_threshold FLOAT 
SET @schedule_adherence_threshold = CONVERT(FLOAT, @prtg)

SELECT 
	count(distinct route_id)
FROM mbta_performance.dbo.daily_metrics
WHERE 
		threshold_id = 'threshold_id_01'
	AND
		time_period_type = 'ALL_DAY'
	AND
		metric_result <= @schedule_adherence_threshold
