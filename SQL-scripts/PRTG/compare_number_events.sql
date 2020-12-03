
--Check expected vs. actual events
--Return number of routes where ratio of actual to scheduled events is less than a specific threshold

SELECT
	COUNT(*)
FROM
	(
	SELECT
		st.service_date
		,st.route_id
		,st.route_type
		,(2*COUNT(st.stop_sequence))-1 as number_scheduled_events 
		,de.number_actual_events 
		,ISNULL(devp.number_actual_events_vp,0) as number_actual_events_vp
		,ISNULL(detu.number_actual_events_tu,0) as number_actual_events_tu
		,de.number_actual_events * 1.0 / ((2*COUNT(st.stop_sequence))-1) as ratio_actual_scheduled

	FROM 
		dbo.daily_stop_times_sec st 
	LEFT JOIN 
		(
			SELECT 
				service_date 
				,route_id
				,route_type 
				,COUNT(event_time) as number_actual_events 
			FROM
				dbo.daily_event 
			GROUP BY 
				service_date 
				,route_id
				,route_type
		) de
		ON
				st.service_date = de.service_date
			AND 
				st.route_id = de.route_id 
			AND 
				st.route_type = de.route_type
	LEFT JOIN 
		(
			SELECT 
				service_date 
				,route_id
				,route_type 
				,COUNT(event_time) as number_actual_events_vp 
			FROM
				dbo.daily_event 
			where 
				event_type IN ('ARR', 'DEP') 
			GROUP BY 
				service_date 
				,route_id
				,route_type
		) devp 
		ON 
				st.service_date = devp.service_date
			AND 
				st.route_id = devp.route_id
			AND 
				st.route_type = devp.route_type
	LEFT JOIN 
		(
			SELECT 
				service_date 
				,route_id
				,route_type 
				,COUNT(event_time) as number_actual_events_tu 
			FROM
				dbo.daily_event 
			where 
				event_type IN ('PRA', 'PRD') 
			GROUP BY 
				service_date 
				,route_id
				,route_type
		) detu
		ON 
				st.service_date = detu.service_date
			AND 
				st.route_id = detu.route_id
			AND 
				st.route_type = detu.route_type 
	WHERE 
		st.route_type IN ('0','1','3')
	GROUP BY 
		st.service_date
		,st.route_id
		,st.route_type
		,de.number_actual_events 
		,devp.number_actual_events_vp
		,detu.number_actual_events_tu
	) t
WHERE
	ratio_actual_scheduled < 0.7