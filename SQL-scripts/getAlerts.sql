
---run this script in the transit-performance database
--USE transit_performance
--GO

--This stored procedure is called by the Alerts API call.  It selects alerts for a particular route, direction, stop and time period.

IF OBJECT_ID('dbo.getAlerts','P') IS NOT NULL
	DROP PROCEDURE dbo.getAlerts
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE dbo.getAlerts
	
	@route_id		VARCHAR(255)
	,@stop_id		VARCHAR(255)
	,@from_time		DATETIME
	,@to_time		DATETIME
	
AS

BEGIN
    SET NOCOUNT ON; 

	IF (DATEDIFF(D,@from_time,@to_time) <= 31)
	BEGIN --if a timespan is less than 31 days, then do the processing, if not return empty set

		SELECT DISTINCT 
			a.alert_id

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
					(route_id = @route_id OR @route_id IS NULL)
				AND
					(e.stop_id = @stop_id OR @stop_id IS NULL)
				AND
					p.active_period_start <= dbo.fnConvertDateTimeToEpoch(@to_time)
				AND
					p.active_period_end >= dbo.fnConvertDateTimeToEpoch(@from_time)

		ORDER BY a.alert_id
	END

END

GO


