
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
		,valid_from				DATETIME2
		,valid_to				DATETIME2
		,cause					VARCHAR(255)
		,effect					VARCHAR(255)
		,header_text			VARCHAR(255)
		,description_text		VARCHAR(1000)
		,url					VARCHAR(255)
		,agency_id				VARCHAR(255)
		,route_id				VARCHAR(255)
		,route_type				INT
		,trip_id				VARCHAR(255)
		,stop_id				VARCHAR(255)
		,active_period_start	INT
		,active_period_end		INT
	)

	IF (DATEDIFF(D,@from_time,@to_time) <= 31)
	BEGIN --if a timespan is less than 31 days, then do the processing, if not return empty set

		IF @include_all_versions = 0

		BEGIN
		
			INSERT INTO @alertstemp
			(
				alert_id				
				,version_id				
				,valid_from				
				,valid_to				
				,cause					
				,effect					
				,header_text			
				,description_text		
				,url					
				,agency_id				
				,route_id				
				,route_type				
				,trip_id				
				,stop_id				
				,active_period_start	
				,active_period_end		
			)

			SELECT DISTINCT 
				a.alert_id
				,a.version_id
				,dbo.fnConvertEpochToDateTime (a.first_file_time) as valid_from
				,dbo.fnConvertEpochToDateTime (a.last_file_time) as valid_to
				,a.cause
				,a.effect
				,a.header_text
				,a.description_text
				,a.url
				,e.agency_id
				,e.route_id
				,e.route_type
				,e.trip_id
				,e.stop_id
				,p.active_period_start
				,p.active_period_end

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
				,valid_from				
				,valid_to				
				,cause					
				,effect					
				,header_text			
				,description_text		
				,url					
				,agency_id				
				,route_id				
				,route_type				
				,trip_id				
				,stop_id				
				,active_period_start	
				,active_period_end		
			)

			SELECT DISTINCT 
				a.alert_id
				,a.version_id
				,dbo.fnConvertEpochToDateTime (a.first_file_time) as valid_from
				,dbo.fnConvertEpochToDateTime (a.last_file_time) as valid_to
				,a.cause
				,a.effect
				,a.header_text
				,a.description_text
				,a.url
				,e.agency_id
				,e.route_id
				,e.route_type
				,e.trip_id
				,e.stop_id
				,p.active_period_start
				,p.active_period_end

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
		,valid_from				
		,valid_to				
		,cause					
		,effect					
		,header_text			
		,description_text		
		,url					
		,agency_id				
		,route_id				
		,route_type				
		,trip_id				
		,stop_id				
		,active_period_start	
		,active_period_end	
	FROM @alertstemp
	ORDER BY alert_id, version_id, active_period_start, active_period_end

	END

END

GO


