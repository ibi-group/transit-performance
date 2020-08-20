--Script Version: Master - 1.0.0.0

--Check daily percent of zombie trip-stops for previous service date
--Zombie trip-stops are stops with actuals but no predictions
--Returns percent of zombie trip-stops system-wide

SELECT pct_no_pred_actual
FROM
(
	SELECT
		st.service_date
		,st.scheduled_trip_stops
		,ISNULL(z.total_trip_stop_no_pred_actual,0) AS total_trip_stop_no_pred_actual
		,ISNULL(z.total_trip_stop_no_pred_actual,0)*1.0/st.scheduled_trip_stops AS pct_no_pred_actual
	FROM 
--total number of trip-stops with actuals but no predictions (zombie)
	(
		SELECT
			service_date
			,count(*) as total_trip_stop_no_pred_actual
		FROM (
			SELECT DISTINCT 
				a.service_date
				,p.trip_id 
				,p.stop_id 
				,p.stop_sequence
				,a.trip_id as actual_trip_id
				,a.stop_id as actual_stop_id 
				,a.stop_sequence as actual_stop_sequence
			FROM dbo.daily_actual a 
			LEFT JOIN dbo.daily_prediction_consolidated p
			ON
					p.service_date = a.service_date 
				AND
					p.trip_id = a.trip_id
				AND
					p.stop_id = a.stop_id
				AND
					p.stop_sequence = a.stop_sequence
			) temp
		WHERE
			stop_id IS NULL
		GROUP BY
			service_date
	) z
--total scheduled trip-stops 
	JOIN 
	(
		SELECT 
			service_date
			,count(*) as scheduled_trip_stops
		FROM dbo.daily_stop_times_sec
		GROUP BY 
			service_date
	) st
	ON 
		z.service_date = st.service_date
) t
