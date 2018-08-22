
---run this script in the transit-performance database
--USE transit_performance
--GO

--This stored procedure is called by the Alerts API call.  It selects alerts for a particular route, direction, stop and time period.

IF OBJECT_ID('dbo.getPastAlerts','P') IS NOT NULL
	DROP PROCEDURE dbo.getPastAlerts
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getPastAlerts
	
	@route_id				VARCHAR(255)
	,@stop_id				VARCHAR(255)
	,@trip_id				VARCHAR(255)
	,@from_time				DATETIME 
	,@to_time				DATETIME 
	,@include_all_versions	BIT  = 0 --default is FALSE, do not include all versions
	
AS

BEGIN
    SET NOCOUNT ON; 

	DECLARE @alertstemp AS TABLE
	(
		alert_id				VARCHAR(255)
		,version_id				INT
	)

	IF (DATEDIFF(D,@from_time,@to_time) <= 31)
	BEGIN --if a timespan is less than 31 days, then do the processing, if not return empty set

		IF @include_all_versions = 0

		BEGIN
		
			INSERT INTO @alertstemp
			(
				alert_id
				,version_id								
			)

			SELECT DISTINCT 
				a.alert_id
				,a.version_id

			FROM
				dbo.rt_alert a
			JOIN
				dbo.rt_alert_active_period p
			ON
					a.alert_id = p.alert_id
				AND
					a.version_id = p.version_id
			JOIN
				dbo.rt_alert_informed_entity e
			ON
					a.alert_id = e.alert_id
				AND
					a.version_id = e.version_id

			WHERE
						(e.route_id = @route_id OR @route_id IS NULL)
					AND
						(e.stop_id = @stop_id OR @stop_id IS NULL)
					AND
						(e.trip_id = @trip_id OR @trip_id IS NULL)
					AND
						p.active_period_start <= dbo.fnConvertDateTimeToEpoch(@to_time)
					AND
						p.active_period_end >= dbo.fnConvertDateTimeToEpoch(@from_time)
					AND
						a.first_file_time <= dbo.fnConvertDateTimeToEpoch(@to_time)
					AND
						a.last_file_time >= dbo.fnConvertDateTimeToEpoch(@from_time)
					AND
						a.closed = 0

		END

		ELSE
		BEGIN

			INSERT INTO @alertstemp
			(
				alert_id
				,version_id									
			)

			SELECT DISTINCT 
				a.alert_id
				,a.version_id

			FROM
				dbo.rt_alert a
			JOIN
				dbo.rt_alert_active_period p
			ON
					a.alert_id = p.alert_id
				AND
					a.version_id = p.version_id
			JOIN
				dbo.rt_alert_informed_entity e
			ON
					a.alert_id = e.alert_id
				AND
					a.version_id = e.version_id

			WHERE
						(e.route_id = @route_id OR @route_id IS NULL)
					AND
						(e.stop_id = @stop_id OR @stop_id IS NULL)
					AND
						(e.trip_id = @trip_id OR @trip_id IS NULL)
					AND
						p.active_period_start <= dbo.fnConvertDateTimeToEpoch(@to_time)
					AND
						p.active_period_end >= dbo.fnConvertDateTimeToEpoch(@from_time)
					AND
						a.closed = 0
		
		END

	SELECT
		alert_id				
		,version_id								
	FROM @alertstemp
	ORDER BY alert_id, version_id

	END

END

GO


