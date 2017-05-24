
---run this script in the transit-performance database
--USE transit_performance
--GO


--This Procedure is called by the scheduleadherence API call. 
--It selects schedule adherence for the requested stop (optionally filtered by route/direction) for the requested time period

IF OBJECT_ID ('getScheduleAdherence_V3') IS NOT NULL
DROP PROCEDURE dbo.getScheduleAdherence_V3
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getScheduleAdherence_V3
	@stop_id		VARCHAR(255)
	,@route_id		VARCHAR(255) NULL
	,@direction_id	INT NULL
	,@from_time		DATETIME
	,@to_time		DATETIME
	

AS


BEGIN
	SET NOCOUNT ON;

	DECLARE @scheduleadherencestemp AS TABLE
	(
		route_id			VARCHAR(255)
		,direction_id		INT
		,trip_id			VARCHAR(255)
		,sch_dt				INT
		,act_dt				INT
		,delay_sec			INT
		,threshold_flag_1	VARCHAR(255)
		,threshold_flag_2	VARCHAR(255)
		,threshold_flag_3	VARCHAR(255)
	)

	IF (DATEDIFF(D,@from_time,@to_time) <= 7)
	BEGIN --if a timespan is less than 7 days, then do the processing, if not return empty set

		INSERT INTO @scheduleadherencestemp
			SELECT
				t.route_id
				,t.direction_id
				,t.trip_id
				,t.sch_dt
				,t.act_dt
				,t.delay_sec
				,t.threshold_flag_1
				,t.threshold_flag_2
				,t.threshold_flag_3
			FROM
			(
				SELECT

					sad.route_id
					,sad.direction_id
					,sad.trip_id
					,CASE
						WHEN sad.stop_order_flag = 1 THEN dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.scheduled_departure_time_sec,sad.service_date))
						ELSE dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.scheduled_arrival_time_sec,sad.service_date))
					END AS sch_dt
					,CASE
						WHEN sad.stop_order_flag = 1 THEN dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.actual_departure_time_sec,sad.service_date))
						ELSE dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.actual_arrival_time_sec,sad.service_date))
					END AS act_dt
					,CASE
						WHEN sad.stop_order_flag = 1 THEN sad.departure_delay_sec
						ELSE sad.arrival_delay_sec
					END AS delay_sec
					,CASE
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec > thc.add_to THEN 'threshold_id_07'
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec > thc.add_to THEN 'threshold_id_07'
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec > thc.add_to THEN 'threshold_id_07'
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec <= thc.add_to THEN NULL
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec <= thc.add_to THEN NULL
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec <= thc.add_to THEN NULL
						ELSE NULL
					END AS threshold_flag_1
					,CASE
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec > thc2.add_to THEN 'threshold_id_08'
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec > thc2.add_to THEN 'threshold_id_08'
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec > thc2.add_to THEN 'threshold_id_08'
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec <= thc2.add_to THEN NULL
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec <= thc2.add_to THEN NULL
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec <= thc2.add_to THEN NULL
						ELSE NULL
					END AS threshold_flag_2
					,CASE
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec > thc3.add_to THEN 'threshold_id_09'
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec > thc3.add_to THEN 'threshold_id_09'
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec > thc3.add_to THEN 'threshold_id_09'
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec <= thc3.add_to THEN NULL
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec <= thc3.add_to THEN NULL
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec <= thc3.add_to THEN NULL
						ELSE NULL
					END AS threshold_flag_3
					,CASE
						WHEN sad.stop_order_flag = 1 THEN DATEADD(s,sad.actual_departure_time_sec,sad.service_date)
						ELSE DATEADD(s,sad.actual_arrival_time_sec,sad.service_date)
					END AS end_date_time
				FROM	dbo.historical_schedule_adherence_disaggregate sad
						,dbo.config_threshold th
						,dbo.config_threshold_calculation thc
						,dbo.config_threshold th2
						,dbo.config_threshold_calculation thc2
						,dbo.config_threshold th3
						,dbo.config_threshold_calculation thc3
				WHERE
					stop_id = @stop_id
					AND (route_id = @route_id
					OR @route_id IS NULL)
					AND (direction_id = @direction_id
					OR @direction_id IS NULL)
					AND th.threshold_id = thc.threshold_id
					AND th.threshold_id = 'threshold_id_07'
					AND th2.threshold_id = thc2.threshold_id
					AND th2.threshold_id = 'threshold_id_08'
					AND th3.threshold_id = thc3.threshold_id
					AND th3.threshold_id = 'threshold_id_09'
			) t
			WHERE
				end_date_time >= @from_time
				AND end_date_time <= @to_time
			GROUP BY
				t.route_id
				,t.direction_id
				,t.trip_id
				,t.sch_dt
				,t.act_dt
				,t.delay_sec
				,t.threshold_flag_1
				,t.threshold_flag_2
				,t.threshold_flag_3
			UNION

			SELECT
				t.route_id
				,t.direction_id
				,t.trip_id
				,t.sch_dt
				,t.act_dt
				,t.delay_sec
				,t.threshold_flag_1
				,t.threshold_flag_2
				,t.threshold_flag_3
			FROM
			(
				SELECT

					sad.route_id
					,sad.direction_id
					,sad.trip_id
					,CASE
						WHEN sad.stop_order_flag = 1 THEN dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.scheduled_departure_time_sec,sad.service_date))
						ELSE dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.scheduled_arrival_time_sec,sad.service_date))
					END AS sch_dt
					,CASE
						WHEN sad.stop_order_flag = 1 THEN dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.actual_departure_time_sec,sad.service_date))
						ELSE dbo.fnConvertDateTimeToEpoch(DATEADD(s,sad.actual_arrival_time_sec,sad.service_date))
					END AS act_dt
					,CASE
						WHEN sad.stop_order_flag = 1 THEN sad.departure_delay_sec
						ELSE sad.arrival_delay_sec
					END AS delay_sec
					,CASE
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec > thc.add_to THEN 'threshold_id_07'
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec > thc.add_to THEN 'threshold_id_07'
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec > thc.add_to THEN 'threshold_id_07'
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec <= thc.add_to THEN NULL
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec <= thc.add_to THEN NULL
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec <= thc.add_to THEN NULL
						ELSE NULL
					END AS threshold_flag_1
					,CASE
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec > thc2.add_to THEN 'threshold_id_08'
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec > thc2.add_to THEN 'threshold_id_08'
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec > thc2.add_to THEN 'threshold_id_08'
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec <= thc2.add_to THEN NULL
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec <= thc2.add_to THEN NULL
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec <= thc2.add_to THEN NULL
						ELSE NULL
					END AS threshold_flag_2
					,CASE
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec > thc3.add_to THEN 'threshold_id_09'
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec > thc3.add_to THEN 'threshold_id_09'
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec > thc3.add_to THEN 'threshold_id_09'
						WHEN sad.stop_order_flag = 1 AND
							sad.departure_delay_sec <= thc3.add_to THEN NULL
						WHEN sad.stop_order_flag = 2 AND
							sad.arrival_delay_sec <= thc3.add_to THEN NULL
						WHEN sad.stop_order_flag = 3 AND
							sad.arrival_delay_sec <= thc3.add_to THEN NULL
						ELSE NULL
					END AS threshold_flag_3
					,CASE
						WHEN sad.stop_order_flag = 1 THEN DATEADD(s,sad.actual_departure_time_sec,sad.service_date)
						ELSE DATEADD(s,sad.actual_arrival_time_sec,sad.service_date)
					END AS end_date_time
				FROM	dbo.today_rt_schedule_adherence_disaggregate sad
						,dbo.config_threshold th
						,dbo.config_threshold_calculation thc
						,dbo.config_threshold th2
						,dbo.config_threshold_calculation thc2
						,dbo.config_threshold th3
						,dbo.config_threshold_calculation thc3
				WHERE
					stop_id = @stop_id
					AND (route_id = @route_id
					OR @route_id IS NULL)
					AND (direction_id = @direction_id
					OR @direction_id IS NULL)
					AND th.threshold_id = thc.threshold_id
					AND th.threshold_id = 'threshold_id_07'
					AND th2.threshold_id = thc2.threshold_id
					AND th2.threshold_id = 'threshold_id_08'
					AND th3.threshold_id = thc3.threshold_id
					AND th3.threshold_id = 'threshold_id_09'
			) t
			WHERE
				end_date_time >= @from_time
				AND end_date_time <= @to_time
			GROUP BY
				t.route_id
				,t.direction_id
				,t.trip_id
				,t.sch_dt
				,t.act_dt
				,t.delay_sec
				,t.threshold_flag_1
				,t.threshold_flag_2
				,t.threshold_flag_3

			ORDER BY
				act_dt

	END--if a timespan is less than 7 days, then do the processing, if not return empty set			
	SELECT
		route_id
		,direction_id
		,trip_id
		,sch_dt
		,act_dt
		,delay_sec
		,threshold_flag_1
		,threshold_flag_2
		,threshold_flag_3
	FROM @scheduleadherencestemp

END





GO