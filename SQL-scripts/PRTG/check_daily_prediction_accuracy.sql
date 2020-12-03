--Script Version: Master - 1.1.0.0

--Check daily prediction quality for previous service date
--Returns number and list of routes that are below prediction quality threshold
--Includes a parameter for the prediction quality threshold, and only returns an error if route prediction quality is below [a configurable] percentage

DECLARE @prediction_quality_threshold FLOAT
SET @prediction_quality_threshold = CONVERT(FLOAT, @prtg)

SELECT
	COUNT(*) as count_routes
	,STRING_AGG(route_id, ', ') as list_routes
	,'0' as number
FROM 
	(
		SELECT 
			route_id
			,AVG(metric_result) as avg_prediction_quality
		FROM mbta_performance.dbo.daily_prediction_metrics_route
		GROUP BY
			route_id
	) t
WHERE
	avg_prediction_quality < @prediction_quality_threshold
