
---run this script in the transit-performance database
--USE transit_performance
--GO

--This stored procedure is called by the getEvents API call.  It selects events for a particular route, direction, stop and time period.

IF OBJECT_ID('dbo.getEvents','P') IS NOT NULL
	DROP PROCEDURE dbo.getEvents
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getEvents
	
	@route_id		VARCHAR(255)
	,@direction_id	INT
	,@stop_id		VARCHAR(255)
	,@from_time		DATETIME
	,@to_time		DATETIME
	
AS

BEGIN
    SET NOCOUNT ON; 

	DECLARE @service_date_from DATE
	SET @service_date_from = dbo.fnConvertDateTimeToServiceDate(@from_time)

	DECLARE @service_date_to DATE
	SET @service_date_to = dbo.fnConvertDateTimeToServiceDate(@to_time)

	IF @service_date_from = @service_date_to --only return results for one day
	
	BEGIN

		DECLARE @service_date  DATE
		SET @service_date = @service_date_from

		SELECT 
			service_date
			,route_id
			,trip_id
			,direction_id
			,e.stop_id
			,s.stop_name
			,e.stop_sequence
			,vehicle_id
			,event_type
			,event_time
			,event_time_sec

		FROM
			dbo.historical_event e
		JOIN
			gtfs.stops s
		ON
			e.stop_id = s.stop_id

		WHERE
					e.service_date = @service_date
				AND
					(route_id = @route_id OR @route_id IS NULL)
				AND
					(direction_id = @direction_id OR @direction_id IS NULL) 
				AND
					e.stop_id = @stop_id
				AND
					event_time >= dbo.fnConvertDateTimeToEpoch(@from_time)
				AND
					event_time <= dbo.fnConvertDateTimeToEpoch(@to_time)
				AND
					suspect_record = 0
		ORDER BY event_time
	END

END

GO


