--Script Version: Master - 1.0.0.0

--Check files with trip gaps for the previous service date
--Returns number of routes with percentage of trip gaps higher than the threshold
--Includes a parameter for the gaps threshold, and only returns an error if route trip gaps is above [a configurable] percentage

DECLARE @pct_file_gap_threshold FLOAT
SET @pct_file_gap_threshold = convert(float, @prtg)

SELECT count(DISTINCT route_id)
FROM daily_vehicle_position_metrics_route
WHERE 
		issue = 'TRIP MISSING FROM VEHICLE POSITIONS FILE FOR EXTENDED PERIOD'
	AND
		metric_result > @pct_file_gap_threshold