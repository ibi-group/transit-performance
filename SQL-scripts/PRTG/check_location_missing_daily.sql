--Script Version: Master - 1.0.0.0

--Check locations with missing or zero lat and long data for the previous service date
--Returns number of routes with percentage of missing locations higher than the threshold
--Includes a parameter for the missing locations threshold, and only returns an error if route missing locations is above [a configurable] percentage

DECLARE @pct_location_missing_threshold FLOAT
SET @pct_location_missing_threshold = CONVERT(float, @prtg)

SELECT count(DISTINCT route_id)
FROM daily_vehicle_position_metrics_route
WHERE 
		issue = 'MISSING LOCATION DATA'
	AND
		metric_result >= @pct_location_missing_threshold