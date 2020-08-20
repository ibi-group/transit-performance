--Script Version: Master - 1.0.0.0

--Check daily percent of ghost trip-stops for previous service date
--Ghost trip-stops are stops with predictions but no actuals
--Returns percent of ghost trip-stops system-wide

SELECT pct_pred_no_actual
FROM
(
	SELECT
		st.service_date
		,st.scheduled_trip_stops
		,ISNULL(g.total_trip_stop_pred_no_actual,0) AS total_trip_stop_pred_no_actual
		,ISNULL(g.total_trip_stop_pred_no_actual,0)*1.0/st.scheduled_trip_stops AS pct_pred_no_actual
	FROM 
--total number of trip-stops with predictions but no actuals (ghost)
	(
		SELECT
			service_date
			,count(*) as total_trip_stop_pred_no_actual
		FROM (
			SELECT DISTINCT 
				p.service_date 
				,p.trip_id
				,p.stop_id
				,p.stop_sequence 
				,a.trip_id as actual_trip_id
				,a.stop_id as actual_stop_id
				,a.stop_sequence as actual_stop_sequence
			FROM dbo.daily_prediction_consolidated p
			LEFT JOIN dbo.daily_actual a
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
			actual_stop_id IS NULL
		GROUP BY
			service_date
	) g
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
		g.service_date = st.service_date
) t





