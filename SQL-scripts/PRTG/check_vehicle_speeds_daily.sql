--Script Version: Master - 1.0.0.0

--Check speeds that are too high for the previous service date
--Returns number of routes with percentage of high speeds higher than the threshold
--Includes a parameter for the high speeds threshold, and only returns an error if route high speeds is above [a configurable] percentage

DECLARE @pct_high_speed_threshold FLOAT
SET @pct_high_speed_threshold = CONVERT(float, @prtg)

SELECT count(DISTINCT route_id)
FROM daily_vehicle_position_metrics_route
WHERE 
		issue = 'SPEED TOO HIGH'
	AND
		metric_result > @pct_high_speed_threshold